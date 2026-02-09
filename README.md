# cloud-monitoring

Monitoramento robusto de conectividade e saúde de tráfego dos pivots via MQTT com:

- Assinatura fixa e somente leitura dos tópicos: `cloudv2`, `cloudv2-ping`, `cloud2`, `cloudv2-network`, `cloudv2-info`.
- Auto-descoberta de pivots apenas por mensagens em `cloudv2`.
- Parser resiliente para payload `#<IDP>-<PIVOT_ID>-...$` com descarte seguro de malformados.
- Cálculo dinâmico da mediana de intervalo de `cloudv2` por pivot (janela móvel).
- Status consolidado por pivot com tolerância de 25%.
- Probe ativo seletivo com `#11$` publicando apenas no tópico dinâmico `pivot_id`.
- Dashboard web com visão geral e visão por pivot (timeline, métricas e controle de probe).

## Arquivos principais

- `cloudv2-ping-monitoring.py`: processo principal (MQTT + telemetria + dashboard).
- `cloudv2_telemetry.py`: ingestão, parser, deduplicação, estado, status e persistência.
- `cloudv2_dashboard.py`: servidor HTTP local e APIs (`/api/state`, `/api/pivot/<id>`, `/api/probe-config`).
- `dashboards/index.html`: UI principal e UI detalhada por pivot.
- `cloudv2_config.py`: configuração e normalização.
- `cloudv2_fixture_simulator.py`: fixture/simulador de validação reprodutível.

## Regras críticas de tópicos

- O sistema **não publica** em: `cloudv2`, `cloudv2-ping`, `cloud2`, `cloudv2-network`, `cloudv2-info`.
- O único publish permitido é no tópico dinâmico `pivot_id` do equipamento, com payload `#11$`.

## Regra de desconexão (offline)

Um pivot é considerado **desconectado** quando:

- o tempo desde a última mensagem recebida em qualquer tópico de conectividade monitorado
  (`cloudv2`, `cloudv2-ping`, `cloudv2-info`, `cloudv2-network`)
- excede `1.25 * max_mediana_esperada_por_topico` do próprio pivot.

Ou seja, o limite usa a maior mediana entre esses tópicos para o pivot e aplica fator de tolerância `1.25`.

## Como executar

1. Ajuste `cloudv2-config.json` (broker, porta e parâmetros de monitoramento).
2. Inicie o monitor:

```bash
python cloudv2-ping-monitoring.py
```

Por padrao o hot reload de desenvolvimento ja vem ativo (reinicia o processo em mudanca de `.py/.html/.css/.js`).
Se quiser desativar:

```bash
$env:CLOUDV2_DEV_HOT_RELOAD="0"; python cloudv2-ping-monitoring.py
```

3. Abra:

```text
http://localhost:8008/index.html
```

## Simulador/fixture

O simulador valida:

- auto-descoberta por `cloudv2`;
- deduplicação;
- mediana móvel;
- estados `gray`, `yellow`, `critical`, `red`;
- evento `cloud2` com tecnologia `wifi`;
- ciclo completo de probe (`sent`/`response`/`timeout`);
- garantia de publicação somente em tópico dinâmico.

Execute:

```bash
python cloudv2_fixture_simulator.py
```

O script imprime um JSON com todos os checks e retorna código `0` em sucesso.

## Dashboard

### Visão principal

- Busca por `pivot_id`.
- Filtro de status (`green`, `yellow`, `critical`, `red`, `gray`).
- Ordenação por criticidade e atividade.
- Cards com:
  - `pivot_id`;
  - status;
  - último ping;
  - último pacote `cloudv2`;
  - mediana estimada + número de amostras;
  - último `cloud2` (RSSI/tecnologia/firmware).

### Visão por pivot

- Timeline de conectividade (verde/vermelho) por período:
  - `Total`, `24h`, `48h`, `7d`, `15d`, `30d` ou `Por periodo (De/Ate)`.
  - Verde = conectado, Vermelho = desconectado.
  - Construída com base em eventos `cloudv2-ping`, `cloudv2` e `cloud2`.
- Métricas:
  - quedas em 24h e 7d;
  - última duração de queda;
  - último RSSI/tecnologia (incluindo `wifi`);
  - firmware e datas.
- Controle de probe por pivot:
  - habilitar/desabilitar;
  - ajustar intervalo;
  - salvar via API.

## Configuração relevante

Campos importantes em `cloudv2-config.json`:

- `ping_interval_minutes` (base do cálculo de ping esperado).
- `tolerance_factor` (padrão `1.25`).
- `attention_disconnected_pct_threshold` (padrão `20.0` para status de atenção).
- `critical_disconnected_pct_threshold` (padrão `50.0` para status crítico).
- `cloudv2_median_window` e `cloudv2_min_samples`.
- `probe_default_interval_sec`, `probe_min_interval_sec`, `probe_timeout_factor`.
- `history_retention_hours` (mínimo 24h).
- `dedupe_window_sec`.
- `probe_settings`:

```json
{
  "probe_settings": {
    "PioneiraLEM_2": { "enabled": true, "interval_sec": 300 },
    "NovaBahia_6": { "enabled": false, "interval_sec": 300 }
  }
}
```
