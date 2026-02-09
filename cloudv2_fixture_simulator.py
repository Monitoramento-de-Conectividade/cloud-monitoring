import json
import sys
import time

from cloudv2_config import FIXED_MONITOR_TOPICS, normalize_config
from cloudv2_telemetry import TelemetryStore


def run_fixture():
    config = normalize_config(
        {
            "history_mode": "fresh",
            "dashboard_refresh_sec": 1,
            "history_retention_hours": 24,
            "cloudv2_median_window": 10,
            "cloudv2_min_samples": 3,
            "dedupe_window_sec": 8,
            "probe_default_interval_sec": 120,
            "probe_min_interval_sec": 60,
            "probe_timeout_factor": 1.25,
            "show_pending_ping_pivots": False,
            "attention_disconnected_pct_threshold": 20.0,
            "attention_disconnected_window_hours": 24,
        }
    )

    telemetry = TelemetryStore(config, log_dir="logs_mqtt")
    published = []
    now = 1765000000.0
    checks = []

    def check(condition, message):
        checks.append({"check": message, "ok": bool(condition)})

    def fake_sender(topic, payload):
        published.append({"ts": now, "topic": str(topic), "payload": str(payload)})
        return str(topic) not in FIXED_MONITOR_TOPICS

    def emit(delta_sec, topic, payload):
        nonlocal now
        now += float(delta_sec)
        result = telemetry.process_message(topic, payload, ts=now)
        telemetry.tick(now)
        return result

    telemetry.set_probe_sender(fake_sender)
    telemetry.start()

    # 1) Ping para pivot ainda nao descoberto nao deve criar pivot automaticamente.
    emit(1, "cloudv2-ping", "#10-PendingPivot_1-ping$")
    state_1 = telemetry.get_state_snapshot(now)
    check(len(state_1.get("pivots", [])) == 0, "ping desconhecido nao cria pivot")
    check(len(state_1.get("pending_ping", [])) == 1, "ping desconhecido fica pendente")

    # 2) Mensagem malformada deve ser descartada sem derrubar.
    malformed_result = emit(1, "cloudv2", "sem_formato_valido")
    check(malformed_result.get("accepted") is False, "mensagem malformada descartada")
    check(state_1.get("counts", {}).get("pivots", 0) == 0, "nenhum pivot criado por malformada")

    # 3) Auto-descoberta via cloudv2 e deduplicacao.
    emit(1, "cloudv2", "#01-PioneiraLEM_2-dataA$")
    duplicate_result = emit(1, "cloudv2", "#01-PioneiraLEM_2-dataA$")
    emit(60, "cloudv2", "#01-PioneiraLEM_2-dataB$")
    emit(60, "cloudv2", "#01-PioneiraLEM_2-dataC$")
    emit(60, "cloudv2", "#01-PioneiraLEM_2-dataD$")
    check(duplicate_result.get("accepted") is False, "duplicada descartada por hash+janela")
    state_2 = telemetry.get_state_snapshot(now)
    check(state_2.get("counts", {}).get("pivots", 0) >= 1, "pivot descoberto por cloudv2")

    # 4) cloud2 com tecnologia wifi deve ser aceito.
    emit(5, "cloudv2-ping", "#10-PioneiraLEM_2-ping1$")
    emit(5, "cloud2", "#11-PioneiraLEM_2--67-wifi-180-v2.3.1-2026-02-09$")
    pivot_snapshot = telemetry.get_pivot_snapshot("PioneiraLEM_2", now)
    last_cloud2 = (pivot_snapshot or {}).get("summary", {}).get("last_cloud2") or {}
    check(str(last_cloud2.get("technology", "")).lower() == "wifi", "cloud2 aceita tecnologia wifi")

    # 5) cloud2 para pivot desconhecido nao deve criar pivot.
    emit(5, "cloud2", "#11-UnknownPivot_9--70-LTE-30-v1.0.0-2026-02-09$")
    state_3 = telemetry.get_state_snapshot(now)
    known_ids = {item.get("pivot_id") for item in state_3.get("pivots", [])}
    check("UnknownPivot_9" not in known_ids, "cloud2 desconhecido nao cria pivot")

    # 6) Probe seletivo: enviar #11$ somente no topico dinamico pivot_id.
    telemetry.update_probe_setting("PioneiraLEM_2", True, 120)
    now += 1
    telemetry.tick(now)
    check(len(published) >= 1, "probe #11$ enviado quando habilitado")
    if published:
        check(published[-1]["topic"] == "PioneiraLEM_2", "probe publica no topico dinamico do pivot")
        check(published[-1]["payload"] == "#11$", "payload do probe e #11$")

    # Resposta do probe em cloudv2-network.
    emit(20, "cloudv2-network", "#11-PioneiraLEM_2-RSSI-wifi-ok$")
    pivot_after_response = telemetry.get_pivot_snapshot("PioneiraLEM_2", now)
    probe_after_response = (pivot_after_response or {}).get("summary", {}).get("probe") or {}
    check(probe_after_response.get("last_result") == "response", "probe correlacionado com resposta")
    check(int(probe_after_response.get("sent_count") or 0) == 1, "contador de envios do probe atualizado")
    check(int(probe_after_response.get("response_count") or 0) == 1, "contador de respostas do probe atualizado")
    check(int(probe_after_response.get("latency_sample_count") or 0) == 1, "amostras de delay registradas")
    latency_last = float(probe_after_response.get("latency_last_sec") or 0.0)
    latency_avg = float(probe_after_response.get("latency_avg_sec") or 0.0)
    check(abs(latency_last - 20.0) <= 0.001, "delay da resposta do probe registrado")
    check(abs(latency_avg - 20.0) <= 0.001, "media de delay do probe atualizada")

    # Novo envio e timeout.
    now += 121
    telemetry.tick(now)
    check(len(published) >= 2, "segundo probe enviado apos intervalo")
    now += 151
    telemetry.tick(now)
    pivot_after_timeout = telemetry.get_pivot_snapshot("PioneiraLEM_2", now)
    probe_after_timeout = (pivot_after_timeout or {}).get("summary", {}).get("probe") or {}
    check(probe_after_timeout.get("last_result") == "timeout", "timeout de probe registrado")
    check(int(probe_after_timeout.get("sent_count") or 0) >= 2, "contador de envios inclui novo probe")
    check(int(probe_after_timeout.get("response_count") or 0) == 1, "contador de respostas preservado apos timeout")
    check(int(probe_after_timeout.get("timeout_count") or 0) >= 1, "contador de timeout atualizado")

    # 7) Estados: amarelo e vermelho por inatividade global monitorada.
    emit(1, "cloudv2-ping", "#10-PioneiraLEM_2-ping2$")
    now += 90
    telemetry.tick(now)
    yellow_snapshot = telemetry.get_pivot_snapshot("PioneiraLEM_2", now)
    yellow_summary = (yellow_snapshot or {}).get("summary") or {}
    yellow_status = (yellow_summary.get("status") or {}).get("code")
    check(yellow_status == "yellow", "regra ping ok + sem cloudv2 vira amarelo")

    disconnect_threshold = int(float(yellow_summary.get("disconnect_threshold_sec") or 0))
    if disconnect_threshold < 1:
        disconnect_threshold = 260
    now += disconnect_threshold + 20
    telemetry.tick(now)
    red_snapshot = telemetry.get_pivot_snapshot("PioneiraLEM_2", now)
    red_status = ((red_snapshot or {}).get("summary", {}).get("status") or {}).get("code")
    check(red_status == "red", "inatividade acima da janela global vira vermelho")

    # 8) Regra nova: se percentual desconectado > 20%, status deve virar amarelo (quando nao estiver offline).
    now += 5 * 3600
    telemetry.tick(now)
    emit(1, "cloudv2-ping", "#10-PioneiraLEM_2-ping3$")
    emit(1, "cloudv2", "#01-PioneiraLEM_2-dataE$")
    attention_snapshot = telemetry.get_pivot_snapshot("PioneiraLEM_2", now)
    attention_summary = (attention_snapshot or {}).get("summary") or {}
    attention_status = (attention_summary.get("status") or {}).get("code")
    attention_pct = float(attention_summary.get("attention_disconnected_pct") or 0.0)
    check(attention_pct > 20.0, "percentual desconectado na janela acima de 20%")
    check(attention_status == "yellow", "status vira amarelo com percentual desconectado > 20%")

    # 9) Pivot novo com poucas amostras deve ficar cinza.
    emit(1, "cloudv2", "#01-GrayPivot_1-first$")
    gray_snapshot = telemetry.get_pivot_snapshot("GrayPivot_1", now)
    gray_status = ((gray_snapshot or {}).get("summary", {}).get("status") or {}).get("code")
    check(gray_status == "gray", "pivot novo sem amostras suficientes fica cinza")

    # 10) Garantia final: nenhuma publicacao em topicos fixos.
    check(
        all(item["topic"] not in FIXED_MONITOR_TOPICS for item in published),
        "nenhum probe publicado em topicos fixos",
    )

    telemetry.stop()

    failed = [item for item in checks if not item["ok"]]
    summary = {
        "ok": len(failed) == 0,
        "checks": checks,
        "published_count": len(published),
        "published_topics": sorted({item["topic"] for item in published}),
        "final_status_pioneira": red_status,
        "final_status_graypivot": gray_status,
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    sys.exit(run_fixture())
