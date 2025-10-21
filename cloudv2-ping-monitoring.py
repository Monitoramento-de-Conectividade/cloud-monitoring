import os
import ssl
import time
import json
import random
import threading
from datetime import datetime

from flask import Flask, request, redirect, url_for, render_template_string, jsonify
import paho.mqtt.client as mqtt

# =============================================================================
# Config e Persistência (defaults = tuas env vars atuais)
# =============================================================================

def _split_list(v):
    """Converte entrada de textarea (vírgulas/linhas) em lista limpa."""
    if isinstance(v, list):
        return [x.strip() for x in v if x and x.strip()]
    if not v:
        return []
    raw = v.replace("\r", "\n").replace(";", ",")
    items = []
    for line in raw.split("\n"):
        for piece in line.split(","):
            piece = piece.strip()
            if piece:
                items.append(piece)
    return items

def load_default_config():
    return {
        "BROKER": os.environ.get("BROKER", "a19mijesri84u2-ats.iot.us-east-1.amazonaws.com"),
        "PORT": int(os.environ.get("PORT", 8883)),
        "TOPICS": ["cloudv2-ping", "cloud2", "cloudv2", "cloudv2-shutdown",
                   "hydrometer-cloudv2", "hydrometer-cloud2", "icrop-cloudv2", "cloudv2-network"],
        "FILTER_NAMES": ["PioneiraLEM_2", "NovaBahia_6", "Savana_16", "ItacirJunior_5",
                         "soilteste_1", "soilteste_2", "OldFriends_12", "GrupoBB_2", "Paineira_16"],
        "CMD_TOPIC": os.environ.get("CMD_TOPIC", "soilteste_2"),
        "INFO_TOPIC": os.environ.get("INFO_TOPIC", "cloudv2-info"),
        "MIN_MINUTES": int(os.environ.get("MIN_MINUTES", 1)),
        "MAX_MINUTES": int(os.environ.get("MAX_MINUTES", 10)),
        "RESPONSE_TIMEOUT_SEC": int(os.environ.get("RESPONSE_TIMEOUT_SEC", 10)),
        # Certs
        "CA_CERT": "amazon_ca.pem",
        "CLIENT_CERT": "device.pem.crt",
        "CLIENT_KEY": "private.pem.key",
        # Web
        "WEB_PORT": int(os.environ.get("WEB_PORT", 8080)),
        "WEB_HOST": os.environ.get("WEB_HOST", "0.0.0.0"),
    }

CONFIG = load_default_config()

# Persistência opcional (arquivo local)
CONFIG_FILE = "ui_config.json"
def save_config_to_disk(cfg):
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[WARN] Não foi possível salvar config em disco: {e}")

def load_config_from_disk():
    global CONFIG
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                stored = json.load(f)
            # Merge: mantêm chaves novas do default
            for k, v in load_default_config().items():
                stored.setdefault(k, v)
            CONFIG = stored
        except Exception as e:
            print(f"[WARN] Não foi possível ler config em disco: {e}")

load_config_from_disk()

# =============================================================================
# Certificados (mesma lógica do teu script)
# =============================================================================

def preparar_certificados(ca_path, cert_path, key_path):
    ca_env = os.environ.get("CA_CERT_CONTENT")
    cert_env = os.environ.get("CLIENT_CERT_CONTENT")
    key_env = os.environ.get("CLIENT_KEY_CONTENT")

    if ca_env and cert_env and key_env:
        with open(ca_path, "w") as f:
            f.write(ca_env)
        with open(cert_path, "w") as f:
            f.write(cert_env)
        with open(key_path, "w") as f:
            f.write(key_env)
        print("[TLS] Certificados carregados a partir de variáveis de ambiente.")
    else:
        print("[TLS] Usando certificados locais (.pem).")

# =============================================================================
# Logs (idêntico ao teu comportamento)
# =============================================================================

LOG_DIR = "logs_mqtt"
os.makedirs(LOG_DIR, exist_ok=True)

def salvar_mensagem(topic, payload):
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    log_path = os.path.join(LOG_DIR, f"{topic}_{data_hoje}.txt")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linha = f"[{timestamp}]\n{payload}\n\n"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(linha)
    print(f"Mensagem salva em {log_path}")

def salvar_resposta_info(payload):
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    log_path = os.path.join(LOG_DIR, f"cloudv2-info_respostas_{data_hoje}.txt")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linha = f"[{timestamp}]\n{payload}\n\n"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(linha)
    print(f"Resposta cloudv2-info salva em {log_path}")

def salvar_envio_resultado(sim_ou_nao):
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    log_path = os.path.join(LOG_DIR, f"envios_11_{data_hoje}.txt")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linha = f"[{timestamp}]\n#11$ - {sim_ou_nao}\n\n"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(linha)
    print(f"Envio registrado em {log_path}: #11$ - {sim_ou_nao}")

# =============================================================================
# MQTT Manager (encapsula teu comportamento original)
# =============================================================================

class MQTTManager:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.client = None
        self._connected = False

        self._esperando_resposta = False
        self._timer_resposta = None
        self._lock = threading.Lock()

        self._agendador_thread = None
        self._agendador_stop = threading.Event()

        self._running = False

    # ----------------------------- Callbacks -----------------------------

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self._connected = True
            print("[MQTT] Conectado ao broker!")
            # (Re)assina todos os tópicos
            for topic in self.cfg["TOPICS"]:
                client.subscribe(topic)
                print(f"[MQTT] Assinado no tópico: {topic}")
            client.subscribe(self.cfg["INFO_TOPIC"])
            print(f"[MQTT] Assinado no tópico: {self.cfg['INFO_TOPIC']}")
            client.subscribe(self.cfg["CMD_TOPIC"])
            print(f"[MQTT] Assinado no tópico: {self.cfg['CMD_TOPIC']}")
        else:
            self._connected = False
            print(f"[MQTT] Falha na conexão. Código: {rc}")

    def _on_disconnect(self, client, userdata, rc):
        self._connected = False
        print(f"[MQTT] Desconectado (rc={rc})")

    def _on_message(self, client, userdata, msg):
        try:
            payload = msg.payload.decode("utf-8", errors="replace")
            print(f"[RX] {msg.topic}: {payload}")

            # Filtragem por nomes (para os tópicos gerais)
            if msg.topic in self.cfg["TOPICS"]:
                if any(name in payload for name in self.cfg["FILTER_NAMES"]):
                    salvar_mensagem(msg.topic, payload)

            # cloudv2-info: sempre salva + controla SIM/NAO
            if msg.topic == self.cfg["INFO_TOPIC"]:
                salvar_resposta_info(payload)
                with self._lock:
                    if self._esperando_resposta:
                        salvar_envio_resultado("SIM")
                        self._esperando_resposta = False
                        if self._timer_resposta is not None:
                            try:
                                self._timer_resposta.cancel()
                            except Exception:
                                pass
                            self._timer_resposta = None

        except Exception as e:
            print(f"[ERR] on_message: {e}")

    # ----------------------- Publicação + Timeout -----------------------

    def _iniciar_timeout_resposta(self):
        def on_timeout():
            with self._lock:
                if self._esperando_resposta:
                    salvar_envio_resultado("NAO")
                self._esperando_resposta = False
                self._timer_resposta = None

        t = threading.Timer(self.cfg["RESPONSE_TIMEOUT_SEC"], on_timeout)
        t.daemon = True
        t.start()
        self._timer_resposta = t

    def _publicar_11(self):
        try:
            self.client.publish(self.cfg["CMD_TOPIC"], "#11$")
            print(f"[TX] #11$ publicado em {self.cfg['CMD_TOPIC']}")
            with self._lock:
                if self._timer_resposta is not None:
                    try:
                        self._timer_resposta.cancel()
                    except Exception:
                        pass
                    self._timer_resposta = None
                self._esperando_resposta = True
                self._iniciar_timeout_resposta()
        except Exception as e:
            print(f"[ERR] publicar #11$: {e}")

    # ---------------------------- Agendador -----------------------------

    def _agendador_loop(self):
        while not self._agendador_stop.is_set():
            # Publica e agenda próximo
            self._publicar_11()
            prox_min = max(1, random.randint(max(1, self.cfg["MIN_MINUTES"]),
                                             max(1, self.cfg["MAX_MINUTES"])))
            print(f"[AGENDADOR] Próximo #11$ em ~{prox_min} minuto(s).")
            # Dorme em blocos pequenos para permitir parada rápida
            for _ in range(prox_min * 60):
                if self._agendador_stop.is_set():
                    break
                time.sleep(1)

    # ----------------------------- Controle -----------------------------

    def start(self):
        if self._running:
            print("[MQTT] Já está em execução.")
            return

        # Sanidade dos intervalos
        if self.cfg["MIN_MINUTES"] > self.cfg["MAX_MINUTES"]:
            self.cfg["MAX_MINUTES"] = self.cfg["MIN_MINUTES"]

        preparar_certificados(self.cfg["CA_CERT"], self.cfg["CLIENT_CERT"], self.cfg["CLIENT_KEY"])

        self.client = mqtt.Client()
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message

        self.client.tls_set(
            ca_certs=self.cfg["CA_CERT"],
            certfile=self.cfg["CLIENT_CERT"],
            keyfile=self.cfg["CLIENT_KEY"],
            tls_version=ssl.PROTOCOL_TLSv1_2
        )

        # Conecta e inicia loop em background
        try:
            self.client.connect(self.cfg["BROKER"], int(self.cfg["PORT"]))
        except Exception as e:
            print(f"[ERR] Conexão inicial: {e}")
            # Continua assim mesmo; o loop tentará reconectar
        self.client.loop_start()

        # Agendador
        self._agendador_stop.clear()
        self._agendador_thread = threading.Thread(target=self._agendador_loop, daemon=True)
        self._agendador_thread.start()

        self._running = True
        print("[MQTT] Iniciado.")

    def stop(self):
        if not self._running:
            print("[MQTT] Já está parado.")
            return

        # Para agendador
        self._agendador_stop.set()
        if self._agendador_thread is not None:
            self._agendador_thread.join(timeout=2.0)
            self._agendador_thread = None

        # Cancela timer de resposta
        with self._lock:
            if self._timer_resposta is not None:
                try:
                    self._timer_resposta.cancel()
                except Exception:
                    pass
                self._timer_resposta = None
            self._esperando_resposta = False

        # Para MQTT
        try:
            self.client.loop_stop()
            self.client.disconnect()
        except Exception:
            pass

        self._connected = False
        self._running = False
        print("[MQTT] Parado.")

    def reconfigure(self, new_cfg: dict):
        """
        Aplica nova configuração em tempo de execução:
        - Atualiza assinaturas (subscribe/unsubscribe) quando conectado
        - Mantém agendador rodando com novos intervalos
        """
        was_running = self._running
        old_topics = set(self.cfg.get("TOPICS", []))
        old_info = self.cfg.get("INFO_TOPIC")
        old_cmd = self.cfg.get("CMD_TOPIC")

        # Atualiza config
        self.cfg.update(new_cfg)

        # Garante coerência de minutos
        if self.cfg["MIN_MINUTES"] > self.cfg["MAX_MINUTES"]:
            self.cfg["MAX_MINUTES"] = self.cfg["MIN_MINUTES"]

        # Atualiza assinaturas se conectado
        if was_running and self._connected and self.client is not None:
            new_topics = set(self.cfg.get("TOPICS", []))
            # Unsubscribe dos que saíram
            for t in old_topics - new_topics:
                try:
                    self.client.unsubscribe(t)
                    print(f"[MQTT] Unsubscribed: {t}")
                except Exception as e:
                    print(f"[WARN] Unsubscribe {t}: {e}")
            # Subscribe dos que entraram
            for t in new_topics - old_topics:
                try:
                    self.client.subscribe(t)
                    print(f"[MQTT] Subscribed: {t}")
                except Exception as e:
                    print(f"[WARN] Subscribe {t}: {e}")
            # INFO/CMD podem ter mudado
            if old_info != self.cfg["INFO_TOPIC"]:
                try:
                    self.client.unsubscribe(old_info)
                except Exception:
                    pass
                self.client.subscribe(self.cfg["INFO_TOPIC"])
                print(f"[MQTT] INFO_TOPIC -> {self.cfg['INFO_TOPIC']}")

            if old_cmd != self.cfg["CMD_TOPIC"]:
                try:
                    self.client.unsubscribe(old_cmd)
                except Exception:
                    pass
                self.client.subscribe(self.cfg["CMD_TOPIC"])
                print(f"[MQTT] CMD_TOPIC -> {self.cfg['CMD_TOPIC']}")

        print("[CFG] Reconfiguração aplicada.")

    @property
    def running(self):
        return self._running

    @property
    def connected(self):
        return self._connected

# Instância global do manager
manager = MQTTManager(CONFIG)

# =============================================================================
# Flask UI
# =============================================================================

HTML = """
<!doctype html>
<html lang="pt-br">
<head>
<meta charset="utf-8">
<title>MQTT Control – Soil</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root{
    --prim:#2d572c; --bg:#f0f9f0; --bd:#a3d3a2;
  }
  body{font-family:Verdana,Arial,sans-serif;background:var(--bg);margin:0;padding:0;color:#1b1b1b}
  header{background:var(--prim);color:#fff;padding:18px 24px}
  header h1{margin:0;font-size:22px}
  .container{max-width:1100px;margin:24px auto;padding:24px;background:#fff;border-radius:16px;
             box-shadow:0 9px 24px rgba(0,0,0,.12);border:4px solid var(--bd)}
  form label{display:block;font-weight:600;margin:14px 0 6px}
  input[type=text], input[type=number], textarea{
    width:100%;padding:12px 14px;border:3px solid var(--bd);border-radius:12px;font-size:14px;outline:none
  }
  textarea{min-height:90px;white-space:pre}
  .grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}
  .row{margin:10px 0}
  .help{color:#444;font-size:12px;margin-top:4px}
  .btns{display:flex;gap:12px;margin-top:16px;flex-wrap:wrap}
  button{
    border:none;border-radius:14px;padding:12px 16px;background:var(--prim);color:#fff;font-weight:700;
    cursor:pointer;box-shadow:0 5px 12px rgba(45,87,44,.25)
  }
  button.secondary{background:#256b24}
  button.warn{background:#9a1e1e}
  .status{display:flex;gap:12px;align-items:center;margin:8px 0 18px}
  .pill{padding:6px 10px;border-radius:999px;color:#fff;font-size:12px}
  .ok{background:#1e8e3e}.bad{background:#b3261e}.idle{background:#8a6a00}
  pre{background:#0b0b0b;color:#0f0;padding:12px;border-radius:10px;overflow:auto}
  footer{color:#444;font-size:12px;text-align:center;margin:18px 0}
</style>
</head>
<body>
<header><h1>MQTT Control • Soil</h1></header>
<div class="container">
  <div class="status">
    <span class="pill {{ 'ok' if running else 'idle' }}">{{ 'Rodando' if running else 'Parado' }}</span>
    <span class="pill {{ 'ok' if connected else 'bad' }}">{{ 'Conectado' if connected else 'Desconectado' }}</span>
  </div>

  <form method="post" action="{{ url_for('apply') }}">
    <div class="grid">
      <div>
        <label>Broker (HOST)</label>
        <input type="text" name="BROKER" value="{{ cfg.BROKER }}">
      </div>
      <div>
        <label>Porta (TLS)</label>
        <input type="number" name="PORT" min="1" max="65535" value="{{ cfg.PORT }}">
      </div>
    </div>

    <div class="grid">
      <div>
        <label>CMD_TOPIC (envio #11$)</label>
        <input type="text" name="CMD_TOPIC" value="{{ cfg.CMD_TOPIC }}">
      </div>
      <div>
        <label>INFO_TOPIC (respostas)</label>
        <input type="text" name="INFO_TOPIC" value="{{ cfg.INFO_TOPIC }}">
      </div>
    </div>

    <div class="row">
      <label>TOPICS (um por linha ou separados por vírgula)</label>
      <textarea name="TOPICS">{{ "\\n".join(cfg.TOPICS) }}</textarea>
      <div class="help">Ex.: cloudv2, cloud2, cloudv2-ping …</div>
    </div>

    <div class="row">
      <label>FILTER_NAMES (um por linha ou separados por vírgula)</label>
      <textarea name="FILTER_NAMES">{{ "\\n".join(cfg.FILTER_NAMES) }}</textarea>
      <div class="help">Somente mensagens contendo um desses nomes serão salvas para os tópicos gerais.</div>
    </div>

    <div class="grid">
      <div>
        <label>MIN_MINUTES</label>
        <input type="number" name="MIN_MINUTES" min="1" value="{{ cfg.MIN_MINUTES }}">
      </div>
      <div>
        <label>MAX_MINUTES</label>
        <input type="number" name="MAX_MINUTES" min="1" value="{{ cfg.MAX_MINUTES }}">
      </div>
    </div>

    <div class="row">
      <label>RESPONSE_TIMEOUT_SEC</label>
      <input type="number" name="RESPONSE_TIMEOUT_SEC" min="1" value="{{ cfg.RESPONSE_TIMEOUT_SEC }}">
      <div class="help">Janela para considerar “SIM”/“NAO” após publicar #11$.</div>
    </div>

    <div class="btns">
      <button type="submit" class="secondary">Aplicar configurações</button>
      <button formaction="{{ url_for('start_mqtt') }}" formmethod="post">Iniciar</button>
      <button formaction="{{ url_for('stop_mqtt') }}" formmethod="post" class="warn">Parar</button>
    </div>
  </form>

  <h3>Config atual (somente leitura)</h3>
  <pre>{{ pretty_cfg }}</pre>
</div>
<footer>Tema verde agrícola • simples e funcional</footer>
</body>
</html>
"""

app = Flask(__name__)

@app.route("/", methods=["GET"])
def index():
    pretty = json.dumps(CONFIG, ensure_ascii=False, indent=2)
    return render_template_string(
        HTML,
        cfg=type("C", (), CONFIG),
        pretty_cfg=pretty,
        running=manager.running,
        connected=manager.connected
    )

@app.route("/apply", methods=["POST"])
def apply():
    new_cfg = dict(CONFIG)  # cópia
    new_cfg["BROKER"] = request.form.get("BROKER", "").strip() or CONFIG["BROKER"]
    try:
        new_cfg["PORT"] = int(request.form.get("PORT", CONFIG["PORT"]))
    except:
        new_cfg["PORT"] = CONFIG["PORT"]

    new_cfg["CMD_TOPIC"] = request.form.get("CMD_TOPIC", "").strip() or CONFIG["CMD_TOPIC"]
    new_cfg["INFO_TOPIC"] = request.form.get("INFO_TOPIC", "").strip() or CONFIG["INFO_TOPIC"]

    new_cfg["TOPICS"] = _split_list(request.form.get("TOPICS", ""))
    if not new_cfg["TOPICS"]:
        new_cfg["TOPICS"] = CONFIG["TOPICS"]

    new_cfg["FILTER_NAMES"] = _split_list(request.form.get("FILTER_NAMES", ""))
    if not new_cfg["FILTER_NAMES"]:
        new_cfg["FILTER_NAMES"] = CONFIG["FILTER_NAMES"]

    try:
        new_cfg["MIN_MINUTES"] = max(1, int(request.form.get("MIN_MINUTES", CONFIG["MIN_MINUTES"])))
    except:
        new_cfg["MIN_MINUTES"] = CONFIG["MIN_MINUTES"]

    try:
        new_cfg["MAX_MINUTES"] = max(1, int(request.form.get("MAX_MINUTES", CONFIG["MAX_MINUTES"])))
    except:
        new_cfg["MAX_MINUTES"] = CONFIG["MAX_MINUTES"]

    try:
        new_cfg["RESPONSE_TIMEOUT_SEC"] = max(1, int(request.form.get("RESPONSE_TIMEOUT_SEC", CONFIG["RESPONSE_TIMEOUT_SEC"])))
    except:
        new_cfg["RESPONSE_TIMEOUT_SEC"] = CONFIG["RESPONSE_TIMEOUT_SEC"]

    # Aplica no manager e salva
    CONFIG.update(new_cfg)
    manager.reconfigure(CONFIG)
    save_config_to_disk(CONFIG)

    return redirect(url_for("index"))

@app.route("/start", methods=["POST"])
def start_mqtt():
    manager.start()
    return redirect(url_for("index"))

@app.route("/stop", methods=["POST"])
def stop_mqtt():
    manager.stop()
    return redirect(url_for("index"))

@app.route("/status", methods=["GET"])
def status():
    return jsonify({
        "running": manager.running,
        "connected": manager.connected,
        "config": CONFIG
    })

if __name__ == "__main__":
    print("[INFO] Acesse a interface em http://localhost:%d/" % CONFIG["WEB_PORT"])
    app.run(host=CONFIG["WEB_HOST"], port=CONFIG["WEB_PORT"])
