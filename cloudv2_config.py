import json
import os


CONFIG_FILE_ENV = "CONFIG_FILE"
DEFAULT_CONFIG_FILE = "cloudv2-config.json"

DEFAULT_CONFIG = {
    "broker": "a19mijesri84u2-ats.iot.us-east-1.amazonaws.com",
    "port": 8883,
    "topics": [
        "cloudv2-ping",
        "cloud2",
        "cloudv2",
        "cloudv2-shutdown",
        "hydrometer-cloudv2",
        "hydrometer-cloud2",
        "icrop-cloudv2",
        "cloudv2-network",
        "configurado",
        "padrao",
    ],
    "filter_names": [
        "PioneiraLEM_1",
        "NovaBahia_6",
        "Savana_16",
        "ItacirJunior_5",
        "soilteste_1",
        "soilteste_2",
        "TerraNostra_4",
        "GrupoBB_2",
        "Paineira_16",
        "Dileta_1",
    ],
    "cmd_topic": "PioneiraLEM_1",
    "info_topic": "cloudv2-info",
    "min_minutes": 1,
    "max_minutes": 10,
    "response_timeout_sec": 10,
    "ping_interval_minutes": 3,
    "ping_topic": "cloudv2-ping",
    "dashboard_enabled": True,
    "dashboard_port": 8008,
    "dashboard_refresh_sec": 5,
    "history_mode": "merge",
}


def get_config_file_path():
    return os.environ.get(CONFIG_FILE_ENV, DEFAULT_CONFIG_FILE)


def _to_int(value, fallback, minimum=None):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = fallback
    if minimum is not None and parsed < minimum:
        return minimum
    return parsed


def _to_bool(value, fallback):
    if isinstance(value, bool):
        return value
    if value is None:
        return fallback
    text = str(value).strip().lower()
    if text in ("1", "true", "yes", "y", "on"):
        return True
    if text in ("0", "false", "no", "n", "off"):
        return False
    return fallback


def _normalize_string_list(values):
    if isinstance(values, str):
        values = values.replace("\n", ",").split(",")
    if not isinstance(values, list):
        return []

    normalized = []
    for item in values:
        as_text = str(item).strip()
        if as_text and as_text not in normalized:
            normalized.append(as_text)
    return normalized


def _normalize_history_mode(value):
    text = str(value or "").strip().lower()
    if text in ("fresh", "new", "zero", "reset", "novo", "zerar", "iniciar_do_zero"):
        return "fresh"
    return "merge"


def _read_config_file(path):
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as file:
            loaded = json.load(file)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"Falha ao carregar configuracao de {path}: {exc}")
        return {}

    if not isinstance(loaded, dict):
        print(f"Arquivo de configuracao invalido (esperado objeto JSON): {path}")
        return {}
    return loaded


def _apply_env_overrides(config):
    overrides = {
        "BROKER": "broker",
        "PORT": "port",
        "CMD_TOPIC": "cmd_topic",
        "INFO_TOPIC": "info_topic",
        "MIN_MINUTES": "min_minutes",
        "MAX_MINUTES": "max_minutes",
        "RESPONSE_TIMEOUT_SEC": "response_timeout_sec",
        "PING_INTERVAL_MINUTES": "ping_interval_minutes",
        "PING_TOPIC": "ping_topic",
        "DASHBOARD_ENABLED": "dashboard_enabled",
        "DASHBOARD_PORT": "dashboard_port",
        "DASHBOARD_REFRESH_SEC": "dashboard_refresh_sec",
        "HISTORY_MODE": "history_mode",
    }
    for env_name, config_key in overrides.items():
        env_value = os.environ.get(env_name)
        if env_value is not None and env_value != "":
            config[config_key] = env_value

    topics_env = os.environ.get("TOPICS")
    if topics_env:
        config["topics"] = topics_env

    filters_env = os.environ.get("FILTER_NAMES")
    if filters_env:
        config["filter_names"] = filters_env


def normalize_config(raw_config):
    base = dict(DEFAULT_CONFIG)
    for key in DEFAULT_CONFIG:
        if key in raw_config:
            base[key] = raw_config[key]

    base["broker"] = str(base.get("broker", DEFAULT_CONFIG["broker"])).strip() or DEFAULT_CONFIG["broker"]
    base["cmd_topic"] = str(base.get("cmd_topic", DEFAULT_CONFIG["cmd_topic"])).strip() or DEFAULT_CONFIG["cmd_topic"]
    base["info_topic"] = str(base.get("info_topic", DEFAULT_CONFIG["info_topic"])).strip() or DEFAULT_CONFIG["info_topic"]
    base["ping_topic"] = str(base.get("ping_topic", DEFAULT_CONFIG["ping_topic"])).strip() or DEFAULT_CONFIG["ping_topic"]
    base["history_mode"] = _normalize_history_mode(base.get("history_mode", DEFAULT_CONFIG["history_mode"]))

    base["port"] = _to_int(base.get("port"), DEFAULT_CONFIG["port"], minimum=1)
    base["min_minutes"] = _to_int(base.get("min_minutes"), DEFAULT_CONFIG["min_minutes"], minimum=1)
    base["max_minutes"] = _to_int(base.get("max_minutes"), DEFAULT_CONFIG["max_minutes"], minimum=1)
    base["response_timeout_sec"] = _to_int(
        base.get("response_timeout_sec"),
        DEFAULT_CONFIG["response_timeout_sec"],
        minimum=1,
    )
    base["ping_interval_minutes"] = _to_int(
        base.get("ping_interval_minutes"),
        DEFAULT_CONFIG["ping_interval_minutes"],
        minimum=1,
    )
    base["dashboard_port"] = _to_int(
        base.get("dashboard_port"),
        DEFAULT_CONFIG["dashboard_port"],
        minimum=1,
    )
    base["dashboard_refresh_sec"] = _to_int(
        base.get("dashboard_refresh_sec"),
        DEFAULT_CONFIG["dashboard_refresh_sec"],
        minimum=1,
    )
    base["dashboard_enabled"] = _to_bool(
        base.get("dashboard_enabled"),
        DEFAULT_CONFIG["dashboard_enabled"],
    )

    if base["min_minutes"] > base["max_minutes"]:
        base["min_minutes"], base["max_minutes"] = base["max_minutes"], base["min_minutes"]

    base["topics"] = _normalize_string_list(base.get("topics")) or list(DEFAULT_CONFIG["topics"])
    base["filter_names"] = _normalize_string_list(base.get("filter_names")) or list(DEFAULT_CONFIG["filter_names"])
    if base["ping_topic"] and base["ping_topic"] not in base["topics"]:
        base["topics"].append(base["ping_topic"])
    return base


def load_editable_config(config_path=None):
    path = config_path or get_config_file_path()
    loaded = _read_config_file(path)
    return normalize_config(loaded)


def load_runtime_config(config_path=None):
    path = config_path or get_config_file_path()
    loaded = _read_config_file(path)
    _apply_env_overrides(loaded)
    return normalize_config(loaded)


def save_config(config, config_path=None):
    path = config_path or get_config_file_path()
    normalized = normalize_config(config)
    with open(path, "w", encoding="utf-8") as file:
        json.dump(normalized, file, indent=2, ensure_ascii=False)
    return normalized
