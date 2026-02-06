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
CMD_TOPIC = runtime_config["cmd_topic"]
INFO_TOPIC = runtime_config["info_topic"]
MIN_MINUTES = runtime_config["min_minutes"]
MAX_MINUTES = runtime_config["max_minutes"]
RESPONSE_TIMEOUT_SEC = runtime_config["response_timeout_sec"]
PING_INTERVAL_MINUTES = runtime_config["ping_interval_minutes"]
PING_TOPIC = runtime_config["ping_topic"]
DASHBOARD_ENABLED = runtime_config["dashboard_enabled"]
DASHBOARD_PORT = runtime_config["dashboard_port"]
DASHBOARD_REFRESH_SEC = runtime_config["dashboard_refresh_sec"]
HISTORY_MODE = runtime_config["history_mode"]

print(f"Arquivo de configuracao ativo: {get_config_file_path()}")

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
    if CMD_TOPIC and CMD_TOPIC not in pivot_ids:
        pivot_ids.append(CMD_TOPIC)
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


def salvar_resposta_info(payload):
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    log_path = os.path.join(LOG_DIR, f"cloudv2-info_respostas_{data_hoje}.txt")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linha = f"[{timestamp}]\n{payload}\n\n"
    with open(log_path, "a", encoding="utf-8") as file:
        file.write(linha)
    print(f"Resposta cloudv2-info salva em {log_path}")


def salvar_envio_resultado(sim_ou_nao):
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    log_path = os.path.join(LOG_DIR, f"envios_11_{data_hoje}.txt")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linha = f"[{timestamp}]\n#11$ - {sim_ou_nao}\n\n"
    with open(log_path, "a", encoding="utf-8") as file:
        file.write(linha)
    print(f"Envio registrado em {log_path}: #11$ - {sim_ou_nao}")


# ------------------------------------
# Estado da funcionalidade #11$
# ------------------------------------

_esperando_resposta = False
_timer_resposta = None
_lock = threading.Lock()


def _iniciar_timeout_resposta():
    """Inicia o timer para marcar 'NAO' se nao houver resposta."""
    global _timer_resposta

    def on_timeout():
        global _esperando_resposta, _timer_resposta
        with _lock:
            if _esperando_resposta:
                salvar_envio_resultado("NAO")
                if telemetry:
                    telemetry.record_ping_result(CMD_TOPIC, ok=False, ts=time.time(), source="timeout")
            _esperando_resposta = False
            _timer_resposta = None

    _timer_resposta = threading.Timer(RESPONSE_TIMEOUT_SEC, on_timeout)
    _timer_resposta.daemon = True
    _timer_resposta.start()


def _publicar_11(client):
    """Publica '#11$' e arma a janela de resposta."""
    global _esperando_resposta, _timer_resposta
    try:
        client.publish(CMD_TOPIC, "#11$")
        print(f"#11$ publicado em {CMD_TOPIC}")
        if telemetry:
            telemetry.record_ping_sent(CMD_TOPIC)
        with _lock:
            if _timer_resposta is not None:
                try:
                    _timer_resposta.cancel()
                except Exception:
                    pass
                _timer_resposta = None
            _esperando_resposta = True
            _iniciar_timeout_resposta()
    except Exception as exc:
        print(f"Erro ao publicar #11$: {exc}")


def _agendador_11(client):
    """Thread que envia #11$ em intervalos aleatorios."""
    while True:
        _publicar_11(client)
        prox_min = random.randint(MIN_MINUTES, MAX_MINUTES)
        print(f"Proximo #11$ em ~{prox_min} minuto(s).")
        time.sleep(prox_min * 60)


# ------------------------------------
# Callbacks MQTT
# ------------------------------------

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Conectado ao broker!")

        subscriptions = []
        for topic in TOPICS + [INFO_TOPIC, CMD_TOPIC, PING_TOPIC]:
            topic = str(topic).strip()
            if topic and topic not in subscriptions:
                subscriptions.append(topic)

        for topic in subscriptions:
            client.subscribe(topic)
            print(f"Assinado no topico: {topic}")
    else:
        print(f"Falha na conexao. Codigo: {rc}")


def on_message(client, userdata, msg):
    global _esperando_resposta, _timer_resposta
    try:
        payload = msg.payload.decode("utf-8")
        print(f"Recebido em {msg.topic}: {payload}")

        if telemetry:
            telemetry.record_message(msg.topic, payload)

        if any(name in payload for name in FILTER_NAMES):
            salvar_mensagem(msg.topic, payload)

        if msg.topic == INFO_TOPIC:
            salvar_resposta_info(payload)
            with _lock:
                if _esperando_resposta:
                    salvar_envio_resultado("SIM")
                    if telemetry:
                        pivot_id = CMD_TOPIC
                        matches = telemetry.detect_pivots(payload)
                        if matches:
                            pivot_id = matches[0]
                        telemetry.record_ping_result(pivot_id, ok=True, ts=time.time(), source="info")
                    _esperando_resposta = False
                    if _timer_resposta is not None:
                        try:
                            _timer_resposta.cancel()
                        except Exception:
                            pass
                        _timer_resposta = None

    except Exception as exc:
        print(f"Erro ao processar mensagem: {exc}")


# ------------------------------------
# Inicializacao
# ------------------------------------

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
