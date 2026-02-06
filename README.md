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

1. Instale as dependencias:
   - `python -m pip install -r requirements.txt`
2. Execute:
   - `python cloudv2-ping-monitoring.py`

O monitor carrega automaticamente `cloudv2-config.json`.
Se variaveis de ambiente estiverem definidas (`BROKER`, `PORT`, etc.), elas sobrescrevem o arquivo.

## Painel HTML (dashboard)

O monitor gera um painel em `dashboards/` e inicia um servidor HTTP.

- Host padrao: `0.0.0.0` (configuravel em `dashboard_host` ou `DASHBOARD_HOST`).
- Porta padrao: `8008` (configuravel em `dashboard_port` ou `DASHBOARD_PORT`).
- Pagina inicial: `http://<IP_DO_SERVIDOR>:8008/index.html`
- Cada pivot possui seu proprio arquivo HTML.
- O painel atualiza automaticamente sem precisar reiniciar o monitor.
- O modo de historico respeita `history_mode`: `merge` reaproveita logs anteriores no dashboard e `fresh` inicia o dashboard zerado para um novo monitoramento.

## Deploy Linux (externo e sem historico)

1. Crie ambiente e instale dependencias:
   - `python3 -m venv .venv`
   - `source .venv/bin/activate`
   - `python -m pip install --upgrade pip`
   - `python -m pip install -r requirements.txt`
2. Configure ambiente para deploy:
   - `export HISTORY_MODE=fresh`
   - `export DASHBOARD_HOST=0.0.0.0`
   - `export DASHBOARD_PORT=8008`
   - (opcional) `export CONFIG_FILE=/caminho/para/cloudv2-config.json`
3. Inicie o monitor:
   - `python cloudv2-ping-monitoring.py`
4. Libere a porta no firewall/security group do servidor (`8008/tcp`).
5. Acesse de qualquer local:
   - `http://<IP_PUBLICO_DO_SERVIDOR>:8008/index.html`

Variaveis uteis para deploy:
- `HISTORY_MODE`: `fresh` (inicia sem historico antigo) ou `merge`.
- `DASHBOARD_HOST`: `0.0.0.0` para acesso externo, `127.0.0.1` para acesso local.
- `DASHBOARD_PORT`: porta HTTP do dashboard.

## Servico systemd (24/7 com restart automatico)

O repositorio inclui:
- `deploy/systemd/cloud-monitoring.service`
- `deploy/systemd/cloud-monitoring.env.example`

Passo a passo sugerido (Ubuntu/Debian):

1. Preparar app em `/opt/cloud-monitoring`:
   - `sudo mkdir -p /opt/cloud-monitoring`
   - copie os arquivos do projeto para `/opt/cloud-monitoring`
   - `cd /opt/cloud-monitoring`
   - `python3 -m venv .venv`
   - `source .venv/bin/activate`
   - `python -m pip install --upgrade pip`
   - `python -m pip install -r requirements.txt`
2. Criar usuario de servico:
   - `sudo useradd --system --home /opt/cloud-monitoring --shell /usr/sbin/nologin cloud-monitor || true`
   - `sudo chown -R cloud-monitor:cloud-monitor /opt/cloud-monitoring`
3. Instalar unit e arquivo de ambiente:
   - `sudo install -D -m 0644 deploy/systemd/cloud-monitoring.service /etc/systemd/system/cloud-monitoring.service`
   - `sudo install -D -m 0644 deploy/systemd/cloud-monitoring.env.example /etc/cloud-monitoring/cloud-monitoring.env`
   - `sudo nano /etc/cloud-monitoring/cloud-monitoring.env`
4. Ativar servico:
   - `sudo systemctl daemon-reload`
   - `sudo systemctl enable --now cloud-monitoring`
5. Operacao e diagnostico:
   - status: `sudo systemctl status cloud-monitoring --no-pager`
   - logs: `sudo journalctl -u cloud-monitoring -f`
   - restart: `sudo systemctl restart cloud-monitoring`

Se seu caminho de deploy for diferente de `/opt/cloud-monitoring`, ajuste `WorkingDirectory` e `ExecStart` no arquivo `cloud-monitoring.service` antes de ativar.

## PWA (instalar no celular)

O dashboard agora gera automaticamente os arquivos PWA em `dashboards/`:
- `manifest.json`
- `service-worker.js`
- `pwa.js`
- `icons/icon-192.png`
- `icons/icon-512.png`

Para instalar no Android:
1. Publique o dashboard em `HTTPS` (obrigatorio para instalacao PWA fora de `localhost`).
2. Abra `https://SEU_DOMINIO/index.html` no Chrome Android.
3. Toque em `Instalar app` (botao do painel) ou no menu do Chrome (`Instalar app`).

Observacao:
- Se abrir por IP sem HTTPS, o painel funciona normalmente, mas a instalacao PWA pode nao aparecer.

## Android APK (Capacitor/TWA)

Foi adicionada a estrutura mobile em `mobile/`:

- Visao geral: `mobile/README.md`
- Capacitor (recomendado para gerar APK rapido): `mobile/capacitor/README.md`
- TWA/Bubblewrap (via PWA + dominio): `mobile/twa/README.md`

Atalho para Capacitor:

1. `cd mobile/capacitor`
2. Ajuste `server.url` em `capacitor.config.json` para seu endpoint `HTTPS`.
3. `npm install`
4. `npm run cap:add:android`
5. `npm run cap:sync`
6. `npm run cap:open` e gere o APK no Android Studio.
