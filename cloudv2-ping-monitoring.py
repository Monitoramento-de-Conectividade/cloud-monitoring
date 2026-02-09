import atexit
import logging
import os
import ssl
import threading

import paho.mqtt.client as mqtt

from cloudv2_config import FIXED_MONITOR_TOPICS, get_config_file_path, load_runtime_config
from cloudv2_dashboard import generate_dashboard_assets, start_dashboard_server
from cloudv2_telemetry import TelemetryStore


PID_FILE = ".cloudv2-monitor.pid"
LOG_DIR = "logs_mqtt"

CA_CERT = "amazon_ca.pem"
CLIENT_CERT = "device.pem.crt"
CLIENT_KEY = "private.pem.key"


runtime_config = load_runtime_config()

BROKER = runtime_config["broker"]
PORT = runtime_config["port"]
MONITOR_TOPICS = tuple(runtime_config.get("monitor_topics") or FIXED_MONITOR_TOPICS)
DASHBOARD_ENABLED = runtime_config["dashboard_enabled"]
DASHBOARD_PORT = runtime_config["dashboard_port"]
DASHBOARD_REFRESH_SEC = runtime_config["dashboard_refresh_sec"]

# Forca os topicos monitorados fixos e sem wildcard.
if tuple(MONITOR_TOPICS) != tuple(FIXED_MONITOR_TOPICS):
    MONITOR_TOPICS = tuple(FIXED_MONITOR_TOPICS)


logger = logging.getLogger("cloudv2.monitor")
telemetry = None
dashboard_server = None
mqtt_client = None
mqtt_connected = threading.Event()


def _configure_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(LOG_DIR, "cloudv2-monitor.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_path, encoding="utf-8"),
        ],
        force=True,
    )
    logger.info("Arquivo de configuracao ativo: %s", get_config_file_path())
    logger.info("Topicos monitorados (somente leitura): %s", ", ".join(MONITOR_TOPICS))


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
        logger.error("Ja existe um monitor em execucao (PID %s).", existing_pid)
        return False
    try:
        with open(PID_FILE, "w", encoding="utf-8") as file:
            file.write(str(current_pid))
    except OSError as exc:
        logger.error("Falha ao criar arquivo de lock do monitor: %s", exc)
        return False
    atexit.register(_release_pid_file)
    return True


def preparar_certificados():
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
        logger.info("Certificados carregados por variaveis de ambiente.")
    else:
        logger.info("Usando certificados locais (.pem).")


def _publish_probe_to_dynamic_topic(pivot_topic, payload):
    topic = str(pivot_topic or "").strip()
    if not topic:
        return False

    # Regra critica: nunca publicar nos topicos fixos monitorados.
    if topic in FIXED_MONITOR_TOPICS:
        logger.error("Bloqueio de seguranca: tentativa de publicar em topico fixo '%s'.", topic)
        return False

    if mqtt_client is None or not mqtt_connected.is_set():
        logger.warning("MQTT ainda nao conectado para publicar probe em %s.", topic)
        return False

    try:
        result = mqtt_client.publish(topic, payload, qos=0, retain=False)
        ok = result.rc == mqtt.MQTT_ERR_SUCCESS
        if ok:
            logger.info("Probe enviado no topico dinamico %s com payload %s", topic, payload)
        else:
            logger.warning("Falha ao publicar probe em %s (rc=%s)", topic, result.rc)
        return ok
    except Exception as exc:
        logger.exception("Erro ao publicar probe em %s: %s", topic, exc)
        return False


def on_connect(client, userdata, flags, rc):
    if rc != 0:
        logger.error("Falha na conexao com broker MQTT. Codigo: %s", rc)
        return

    mqtt_connected.set()
    logger.info("Conectado ao broker MQTT.")
    for topic in MONITOR_TOPICS:
        client.subscribe(topic)
        logger.info("Assinado em topico fixo: %s", topic)


def on_disconnect(client, userdata, rc):
    mqtt_connected.clear()
    logger.warning("Desconectado do broker MQTT (rc=%s).", rc)


def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8", errors="replace")
    except Exception:
        payload = str(msg.payload)

    try:
        if telemetry is not None:
            telemetry.process_message(msg.topic, payload)
    except Exception as exc:
        logger.exception("Erro ao processar mensagem MQTT topic=%s: %s", msg.topic, exc)


def main():
    global telemetry
    global dashboard_server
    global mqtt_client

    _configure_logging()

    if not _acquire_pid_file():
        raise SystemExit(1)

    preparar_certificados()

    telemetry = TelemetryStore(runtime_config, log_dir=LOG_DIR)
    telemetry.set_probe_sender(_publish_probe_to_dynamic_topic)
    telemetry.start()

    if DASHBOARD_ENABLED:
        generate_dashboard_assets(DASHBOARD_REFRESH_SEC)
        dashboard_server = start_dashboard_server(DASHBOARD_PORT, telemetry)
        logger.info("Dashboard ativo em http://localhost:%s/index.html", DASHBOARD_PORT)

    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_message = on_message

    mqtt_client.tls_set(
        ca_certs=CA_CERT,
        certfile=CLIENT_CERT,
        keyfile=CLIENT_KEY,
        tls_version=ssl.PROTOCOL_TLSv1_2,
    )

    logger.info("Conectando ao broker %s:%s ...", BROKER, PORT)
    mqtt_client.connect(BROKER, PORT)

    try:
        mqtt_client.loop_forever()
    except KeyboardInterrupt:
        logger.info("Encerrando monitor por interrupcao do usuario.")
    finally:
        mqtt_connected.clear()
        if telemetry is not None:
            telemetry.stop()
        if dashboard_server is not None:
            try:
                dashboard_server.shutdown()
            except Exception:
                pass


if __name__ == "__main__":
    main()
