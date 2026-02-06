import atexit
import os
import random
import ssl
import threading
import time
from datetime import datetime

import paho.mqtt.client as mqtt

from cloudv2_config import get_config_file_path, load_runtime_config
from cloudv2_dashboard import generate_dashboard_assets, start_dashboard_server
from cloudv2_telemetry import TelemetryStore

# ------------------------------------
# Configuracoes MQTT
# ------------------------------------

runtime_config = load_runtime_config()

BROKER = runtime_config["broker"]
PORT = runtime_config["port"]
TOPICS = runtime_config["topics"]
FILTER_NAMES = runtime_config["filter_names"]
CMD_TOPICS = runtime_config["cmd_topics"]
INFO_TOPICS = runtime_config["info_topics"]
MIN_MINUTES = runtime_config["min_minutes"]
MAX_MINUTES = runtime_config["max_minutes"]
SCHEDULE_MODE = runtime_config["schedule_mode"]
FIXED_MINUTES = runtime_config["fixed_minutes"]
RESPONSE_TIMEOUT_SEC = runtime_config["response_timeout_sec"]
PING_INTERVAL_MINUTES = runtime_config["ping_interval_minutes"]
PING_TOPIC = runtime_config["ping_topic"]
DASHBOARD_ENABLED = runtime_config["dashboard_enabled"]
DASHBOARD_PORT = runtime_config["dashboard_port"]
DASHBOARD_REFRESH_SEC = runtime_config["dashboard_refresh_sec"]
HISTORY_MODE = runtime_config["history_mode"]

print(f"Arquivo de configuracao ativo: {get_config_file_path()}")

PID_FILE = ".cloudv2-monitor.pid"


def _is_process_running(pid):
    if not isinstance(pid, int) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _read_pid_file():
    if not os.path.exists(PID_FILE):
        return None
    try:
        with open(PID_FILE, "r", encoding="utf-8") as file:
            content = file.read().strip()
        if not content:
            return None
        return int(content)
    except (OSError, ValueError):
        return None


def _release_pid_file():
    current_pid = os.getpid()
    pid_in_file = _read_pid_file()
    if pid_in_file != current_pid:
        return
    try:
        os.remove(PID_FILE)
    except OSError:
        pass


def _acquire_pid_file():
    current_pid = os.getpid()
    existing_pid = _read_pid_file()
    if existing_pid and existing_pid != current_pid and _is_process_running(existing_pid):
        print(f"Ja existe um monitor em execucao (PID {existing_pid}). Encerrando esta instancia.")
        return False
    try:
        with open(PID_FILE, "w", encoding="utf-8") as file:
            file.write(str(current_pid))
    except OSError as exc:
        print(f"Falha ao criar arquivo de lock do monitor: {exc}")
        return False
    atexit.register(_release_pid_file)
    return True

# ------------------------------------
# Gerenciar certificados
# ------------------------------------

# Caminhos dos arquivos locais
CA_CERT = "amazon_ca.pem"
CLIENT_CERT = "device.pem.crt"
CLIENT_KEY = "private.pem.key"


def preparar_certificados():
    """
    Se as variaveis de ambiente de certificados existirem (Render),
    grava em arquivos temporarios. Se nao existirem, assume que os
    arquivos locais ja estao disponiveis (ambiente de desenvolvimento).
    """
    ca_env = os.environ.get("CA_CERT_CONTENT")
    cert_env = os.environ.get("CLIENT_CERT_CONTENT")
    key_env = os.environ.get("CLIENT_KEY_CONTENT")

    if ca_env and cert_env and key_env:
        with open(CA_CERT, "w", encoding="utf-8") as file:
            file.write(ca_env)
        with open(CLIENT_CERT, "w", encoding="utf-8") as file:
            file.write(cert_env)
        with open(CLIENT_KEY, "w", encoding="utf-8") as file:
            file.write(key_env)
        print("Certificados carregados por variaveis de ambiente.")
    else:
        print("Usando certificados locais (.pem).")


# ------------------------------------
# Logs
# ------------------------------------

LOG_DIR = "logs_mqtt"
os.makedirs(LOG_DIR, exist_ok=True)

telemetry = None
if DASHBOARD_ENABLED:
    pivot_ids = list(FILTER_NAMES)
    for cmd_topic in CMD_TOPICS:
        if cmd_topic not in pivot_ids:
            pivot_ids.append(cmd_topic)
    generate_dashboard_assets(pivot_ids, DASHBOARD_REFRESH_SEC)
    telemetry = TelemetryStore(runtime_config, pivot_ids, LOG_DIR)
    telemetry.start()
    if HISTORY_MODE == "fresh":
        print("Dashboard iniciado em modo novo monitoramento (sem historico salvo).")
    else:
        print("Dashboard iniciado em modo historico acumulado.")
    try:
        start_dashboard_server(DASHBOARD_PORT)
        print(f"Dashboard ativo em http://localhost:{DASHBOARD_PORT}/index.html")
    except OSError as exc:
        print(f"Falha ao iniciar o dashboard: {exc}")


def salvar_mensagem(topic, payload):
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    log_path = os.path.join(LOG_DIR, f"{topic}_{data_hoje}.txt")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linha = f"[{timestamp}]\n{payload}\n\n"

    with open(log_path, "a", encoding="utf-8") as file:
        file.write(linha)

    print(f"Mensagem salva em {log_path}")


def _safe_topic_filename(topic):
    return str(topic).strip().replace("/", "_")


def salvar_resposta_info(topic, payload):
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    safe_topic = _safe_topic_filename(topic)
    log_path = os.path.join(LOG_DIR, f"{safe_topic}_respostas_{data_hoje}.txt")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linha = f"[{timestamp}]\n{payload}\n\n"
    with open(log_path, "a", encoding="utf-8") as file:
        file.write(linha)
    print(f"Resposta {topic} salva em {log_path}")


def salvar_envio_resultado(cmd_topic, sim_ou_nao):
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    log_path = os.path.join(LOG_DIR, f"envios_11_{data_hoje}.txt")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linha = f"[{timestamp}]\n#11$ [{cmd_topic}] - {sim_ou_nao}\n\n"
    with open(log_path, "a", encoding="utf-8") as file:
        file.write(linha)
    print(f"Envio registrado em {log_path}: #11$ [{cmd_topic}] - {sim_ou_nao}")


# ------------------------------------
# Estado da funcionalidade #11$
# ------------------------------------

_aguardando_resposta = set()
_timers_resposta = {}
_lock = threading.Lock()


def _iniciar_timeout_resposta(cmd_topic):
    """Inicia o timer para marcar 'NAO' se nao houver resposta para o topico."""
    def on_timeout():
        with _lock:
            if cmd_topic in _aguardando_resposta:
                salvar_envio_resultado(cmd_topic, "NAO")
                if telemetry:
                    telemetry.record_ping_result(cmd_topic, ok=False, ts=time.time(), source="timeout")
            _aguardando_resposta.discard(cmd_topic)
            _timers_resposta.pop(cmd_topic, None)

    timer = threading.Timer(RESPONSE_TIMEOUT_SEC, on_timeout)
    timer.daemon = True
    _timers_resposta[cmd_topic] = timer
    timer.start()


def _publicar_11(client, cmd_topic):
    """Publica '#11$' e arma a janela de resposta por topico de comando."""
    try:
        client.publish(cmd_topic, "#11$")
        print(f"#11$ publicado em {cmd_topic}")
        if telemetry:
            telemetry.record_ping_sent(cmd_topic)
        with _lock:
            timer = _timers_resposta.get(cmd_topic)
            if timer is not None:
                try:
                    timer.cancel()
                except Exception:
                    pass
                _timers_resposta.pop(cmd_topic, None)
            _aguardando_resposta.add(cmd_topic)
            _iniciar_timeout_resposta(cmd_topic)
    except Exception as exc:
        print(f"Erro ao publicar #11$ em {cmd_topic}: {exc}")


def _agendador_11(client):
    """Thread que envia #11$ em intervalos aleatorios ou fixos."""
    while True:
        if not CMD_TOPICS:
            print("Nenhum topico de comando (#11$) configurado.")
            time.sleep(60)
            continue
        for cmd_topic in CMD_TOPICS:
            _publicar_11(client, cmd_topic)
            time.sleep(0.2)

        if SCHEDULE_MODE == "fixed":
            prox_min = FIXED_MINUTES
            print(f"Proximo #11$ em {prox_min} minuto(s) (fixo).")
        else:
            prox_min = random.randint(MIN_MINUTES, MAX_MINUTES)
            print(f"Proximo #11$ em ~{prox_min} minuto(s) (aleatorio).")
        time.sleep(prox_min * 60)


# ------------------------------------
# Callbacks MQTT
# ------------------------------------

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Conectado ao broker!")

        subscriptions = []
        for topic in TOPICS + CMD_TOPICS + INFO_TOPICS + [PING_TOPIC]:
            topic = str(topic).strip()
            if topic and topic not in subscriptions:
                subscriptions.append(topic)

        for topic in subscriptions:
            client.subscribe(topic)
            print(f"Assinado no topico: {topic}")
    else:
        print(f"Falha na conexao. Codigo: {rc}")


def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8")
        print(f"Recebido em {msg.topic}: {payload}")

        if telemetry:
            telemetry.record_message(msg.topic, payload)

        if any(name in payload for name in FILTER_NAMES):
            salvar_mensagem(msg.topic, payload)

        if msg.topic in INFO_TOPICS:
            salvar_resposta_info(msg.topic, payload)
            with _lock:
                responded_topics = []
                for cmd_topic in CMD_TOPICS:
                    if cmd_topic in payload and cmd_topic in _aguardando_resposta:
                        responded_topics.append(cmd_topic)

                # Fallback: se houver somente um topico aguardando, associa a resposta.
                if not responded_topics and len(_aguardando_resposta) == 1:
                    responded_topics.append(next(iter(_aguardando_resposta)))

                for cmd_topic in responded_topics:
                    salvar_envio_resultado(cmd_topic, "SIM")
                    if telemetry:
                        telemetry.record_ping_result(cmd_topic, ok=True, ts=time.time(), source=msg.topic)
                    _aguardando_resposta.discard(cmd_topic)
                    timer = _timers_resposta.pop(cmd_topic, None)
                    if timer is not None:
                        try:
                            timer.cancel()
                        except Exception:
                            pass

    except Exception as exc:
        print(f"Erro ao processar mensagem: {exc}")


# ------------------------------------
# Inicializacao
# ------------------------------------

if not _acquire_pid_file():
    raise SystemExit(1)

preparar_certificados()

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.tls_set(
    ca_certs=CA_CERT,
    certfile=CLIENT_CERT,
    keyfile=CLIENT_KEY,
    tls_version=ssl.PROTOCOL_TLSv1_2,
)

client.connect(BROKER, PORT)
threading.Thread(target=_agendador_11, args=(client,), daemon=True).start()
client.loop_forever()
