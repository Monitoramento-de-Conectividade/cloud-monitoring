import json
import os
import sys
import time

from backend.cloudv2_config import FIXED_MONITOR_TOPICS, normalize_config
from backend.cloudv2_paths import resolve_data_dir
from backend.cloudv2_telemetry import TelemetryStore


def run_fixture():
    fixture_db_path = os.path.join(resolve_data_dir(), "fixture_simulator.sqlite3")
    if os.path.exists(fixture_db_path):
        try:
            os.remove(fixture_db_path)
        except OSError:
            pass

    config = normalize_config(
        {
            "history_mode": "fresh",
            "dashboard_refresh_sec": 1,
            "history_retention_hours": 24,
            "require_apply_to_start": False,
            "cloudv2_median_window": 10,
            "cloudv2_min_samples": 5,
            "enable_background_worker": False,
            "sqlite_db_path": fixture_db_path,
            "dedupe_window_sec": 8,
            "probe_default_interval_sec": 120,
            "probe_min_interval_sec": 60,
            "probe_timeout_factor": 1.25,
            "show_pending_ping_pivots": False,
            "attention_disconnected_pct_threshold": 20.0,
            "critical_disconnected_pct_threshold": 50.0,
            "attention_disconnected_window_hours": 1,
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

    # 7) Estado atual por inatividade global e qualidade por percentual.
    emit(1, "cloudv2-ping", "#10-PioneiraLEM_2-ping2$")
    now += 90
    telemetry.tick(now)
    yellow_snapshot = telemetry.get_pivot_snapshot("PioneiraLEM_2", now)
    yellow_summary = (yellow_snapshot or {}).get("summary") or {}
    yellow_state = (yellow_summary.get("status") or {}).get("code")
    yellow_quality = (yellow_summary.get("quality") or {}).get("code")
    check(
        bool(yellow_summary.get("ping_ok")) and not bool(yellow_summary.get("cloudv2_ok")),
        "regra ping ok + sem cloudv2 detectada",
    )
    check(yellow_state == "gray", "estado permanece em inicial ate completar amostras minimas da mediana")
    check(yellow_quality in ("yellow", "critical"), "qualidade fica em atencao/critico com percentual alto")

    disconnect_threshold = int(float(yellow_summary.get("disconnect_threshold_sec") or 0))
    if disconnect_threshold < 1:
        disconnect_threshold = 260
    now += disconnect_threshold + 20
    telemetry.tick(now)
    red_snapshot = telemetry.get_pivot_snapshot("PioneiraLEM_2", now)
    red_state = ((red_snapshot or {}).get("summary", {}).get("status") or {}).get("code")
    check(red_state == "red", "inatividade acima da janela global vira offline")

    # 8) Qualidade CRITICO: percentual desconectado > 50% (quando nao estiver offline).
    now += 5 * 3600
    telemetry.tick(now)
    emit(1, "cloudv2-ping", "#10-PioneiraLEM_2-ping3$")
    emit(1, "cloudv2", "#01-PioneiraLEM_2-dataE$")
    attention_snapshot = telemetry.get_pivot_snapshot("PioneiraLEM_2", now)
    attention_summary = (attention_snapshot or {}).get("summary") or {}
    attention_quality = (attention_summary.get("quality") or {}).get("code")
    attention_pct = float(attention_summary.get("attention_disconnected_pct") or 0.0)
    check(attention_pct > 50.0, "percentual desconectado na janela acima de 50%")
    check(attention_quality == "critical", "qualidade vira critico com percentual desconectado > 50%")

    # 8.1) Recuperacao automatica para ATENCAO quando percentual cair para <= 50%.
    for _ in range(35):
        emit(60, "cloudv2-ping", "#10-PioneiraLEM_2-recovery-pingA$")
        emit(1, "cloudv2", "#01-PioneiraLEM_2-recovery-dataA$")
    recovery_attention_snapshot = telemetry.get_pivot_snapshot("PioneiraLEM_2", now)
    recovery_attention_summary = (recovery_attention_snapshot or {}).get("summary") or {}
    recovery_attention_quality = (recovery_attention_summary.get("quality") or {}).get("code")
    recovery_attention_pct = float(recovery_attention_summary.get("attention_disconnected_pct") or 0.0)
    check(recovery_attention_pct <= 50.0, "percentual desconectado recua para <= 50%")
    check(recovery_attention_pct > 20.0, "percentual desconectado permanece > 20% no ponto intermediario")
    check(recovery_attention_quality == "yellow", "qualidade recua de critico para atencao automaticamente")

    # 8.2) Recuperacao automatica para SAUDAVEL quando percentual cair para <= 20%.
    for _ in range(15):
        emit(60, "cloudv2-ping", "#10-PioneiraLEM_2-recovery-pingB$")
        emit(1, "cloudv2", "#01-PioneiraLEM_2-recovery-dataB$")
    recovery_healthy_snapshot = telemetry.get_pivot_snapshot("PioneiraLEM_2", now)
    recovery_healthy_summary = (recovery_healthy_snapshot or {}).get("summary") or {}
    recovery_healthy_quality = (recovery_healthy_summary.get("quality") or {}).get("code")
    recovery_healthy_pct = float(recovery_healthy_summary.get("attention_disconnected_pct") or 0.0)
    recovery_healthy_connected = float(recovery_healthy_summary.get("connected_pct") or 0.0)
    check(recovery_healthy_pct <= 20.0, "percentual desconectado recua para <= 20%")
    check(recovery_healthy_connected >= 80.0, "percentual conectado sobe para >= 80%")
    check(recovery_healthy_quality == "green", "qualidade recua para saudavel automaticamente")

    # 8.3) Regra de ATENCAO por topicos auxiliares sem cloudv2 principal.
    emit(1, "cloudv2", "#01-AuxOnlyPivot_1-discovery$")
    now += 3700
    telemetry.tick(now)
    for _ in range(50):
        emit(60, "cloudv2-ping", "#10-AuxOnlyPivot_1-ping$")
    emit(1, "cloudv2-network", "#11-AuxOnlyPivot_1-RSSI-wifi-ok$")
    aux_only_snapshot = telemetry.get_pivot_snapshot("AuxOnlyPivot_1", now)
    aux_only_summary = (aux_only_snapshot or {}).get("summary") or {}
    aux_only_quality = (aux_only_summary.get("quality") or {}).get("code")
    aux_only_disconnected_pct = float(aux_only_summary.get("attention_disconnected_pct") or 0.0)
    check(aux_only_disconnected_pct <= 20.0, "aux-only mantem percentual desconectado em faixa boa")
    check(
        bool(aux_only_summary.get("attention_by_only_aux_topics")),
        "sinaliza condicao sem principal e apenas ping/network/info",
    )
    check(aux_only_quality == "yellow", "qualidade fica em atencao com apenas ping/network/info")

    # 9) Pivot novo com poucas amostras deve ficar cinza.
    emit(1, "cloudv2", "#01-GrayPivot_1-first$")
    gray_snapshot = telemetry.get_pivot_snapshot("GrayPivot_1", now)
    gray_status = ((gray_snapshot or {}).get("summary", {}).get("status") or {}).get("code")
    check(gray_status == "gray", "pivot novo sem amostras suficientes fica cinza")

    # 9.1) Mediana cloudv2 com tolerancia: pequenas variacoes contam na mesma base.
    emit(1, "cloudv2", "#01-MedianTolPivot_1-start$")
    emit(300, "cloudv2", "#01-MedianTolPivot_1-sampleA$")
    emit(310, "cloudv2", "#01-MedianTolPivot_1-sampleB$")
    emit(295, "cloudv2", "#01-MedianTolPivot_1-sampleC$")
    emit(305, "cloudv2", "#01-MedianTolPivot_1-sampleD$")
    median_tol_snapshot = telemetry.get_pivot_snapshot("MedianTolPivot_1", now)
    median_tol_summary = (median_tol_snapshot or {}).get("summary") or {}
    median_tol_value = float(median_tol_summary.get("median_cloudv2_interval_sec") or 0.0)
    median_tol_samples = int(median_tol_summary.get("median_sample_count") or 0)
    check(295.0 <= median_tol_value <= 310.0, "mediana cloudv2 fica proxima de 300s com variacao leve")
    check(median_tol_samples >= 4, "variacoes 300/310/295/305 contam como amostras validas")

    emit(390, "cloudv2", "#01-MedianTolPivot_1-outlier390$")
    outlier_snapshot = telemetry.get_pivot_snapshot("MedianTolPivot_1", now)
    outlier_summary = (outlier_snapshot or {}).get("summary") or {}
    outlier_median = float(outlier_summary.get("median_cloudv2_interval_sec") or 0.0)
    outlier_samples = int(outlier_summary.get("median_sample_count") or 0)
    check(295.0 <= outlier_median <= 310.0, "intervalo 390s nao derruba candidato de ~300s")
    check(outlier_samples == median_tol_samples, "intervalo 390s nao entra como equivalente ao bucket de ~300s")

    emit(600, "cloudv2", "#01-MedianTolPivot_1-switchA$")
    emit(610, "cloudv2", "#01-MedianTolPivot_1-switchB$")
    emit(590, "cloudv2", "#01-MedianTolPivot_1-switchC$")
    emit(605, "cloudv2", "#01-MedianTolPivot_1-switchD$")
    switched_snapshot = telemetry.get_pivot_snapshot("MedianTolPivot_1", now)
    switched_summary = (switched_snapshot or {}).get("summary") or {}
    switched_median = float(switched_summary.get("median_cloudv2_interval_sec") or 0.0)
    switched_samples = int(switched_summary.get("median_sample_count") or 0)
    check(580.0 <= switched_median <= 620.0, "candidato da mediana migra para ~600s com novas amostras")
    check(switched_samples >= 4, "tolerancia reaplica no novo candidato de ~600s")

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
        "final_status_pioneira": ((recovery_healthy_snapshot or {}).get("summary", {}).get("status") or {}).get("code"),
        "final_quality_pioneira": recovery_healthy_quality,
        "final_status_graypivot": gray_status,
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    sys.exit(run_fixture())
