# cloud-monitoring

Monitoramento MQTT com logs e ping ativo (`#11$`) para validar resposta.

## Arquivos principais

- `cloudv2-ping-monitoring.py`: monitor MQTT (processo principal).
- `cloudv2-config-ui.py`: interface grafica para editar configuracao.
- `cloudv2-config.json`: configuracao persistida (topicos, filtros e parametros).
- `cloudv2_config.py`: modulo compartilhado de carga/salvamento de configuracao.

## Como usar a interface

1. Execute `python cloudv2-config-ui.py`.
2. Edite os campos principais (broker, tempos e topico de resposta preciso).
3. Preencha `Monitoramento mais preciso` com um ou mais destinos para envio do `#11$`.
4. Edite `Monitoramento leve (nomes)` e `Topicos de resposta` conforme a necessidade.
5. Defina o modo de envio do `#11$`:
   - `Aleatorio (min/max)` usa `min_minutes` e `max_minutes`.
   - `Fixo (periodico)` usa `fixed_minutes`.
6. Escolha o modo de historico `Juntar com historico salvo` (`history_mode = merge`) ou `Comecar monitoramento novo (zero)` (`history_mode = fresh`).
7. Clique em `Salvar configuracao` para apenas salvar, ou em `Iniciar Monitoramento` para salvar, iniciar o monitor e abrir o dashboard local.
8. Se ja existir monitor rodando, a interface pergunta se deseja reiniciar a instancia para aplicar a nova configuracao.

## Como iniciar o monitor

Execute `python cloudv2-ping-monitoring.py`.

O monitor carrega automaticamente `cloudv2-config.json`.
Se variaveis de ambiente estiverem definidas (`BROKER`, `PORT`, etc.), elas sobrescrevem o arquivo.

## Painel HTML (dashboard)

O monitor gera um painel em `dashboards/` e inicia um servidor HTTP local.

- Porta padrao: `8008` (configuravel em `dashboard_port`).
- Pagina inicial: `http://localhost:8008/index.html`
- Cada pivot possui seu proprio arquivo HTML.
- O painel atualiza automaticamente sem precisar reiniciar o monitor.
- O modo de historico respeita `history_mode`: `merge` reaproveita logs anteriores no dashboard e `fresh` inicia o dashboard zerado para um novo monitoramento.
