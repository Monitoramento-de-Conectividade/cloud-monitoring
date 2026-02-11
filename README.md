# cloud-monitoring

Monitoramento robusto de conectividade e saúde de tráfego dos pivots via MQTT com:

- Assinatura fixa e somente leitura dos tópicos: `cloudv2`, `cloudv2-ping`, `cloud2`, `cloudv2-network`, `cloudv2-info`.
- Auto-descoberta de pivots apenas por mensagens em `cloudv2`.
- Parser resiliente para payload `#<IDP>-<PIVOT_ID>-...$` com descarte seguro de malformados.
- Cálculo dinâmico da mediana de intervalo de `cloudv2` por pivot (janela móvel).
- Status consolidado por pivot com tolerância de 25%.
- Probe ativo seletivo com `#11$` publicando apenas no tópico dinâmico `pivot_id`.
- Dashboard web com visão geral e visão por pivot (timeline, métricas e controle de probe).

## Estrutura

- `backend/`: backend (monitor MQTT, telemetria, persistência, dashboard HTTP e utilitários).
- `frontend/`: frontend (assets `index.html`, `dashboard.css`, `dashboard.js`).
- Wrappers legados no root: `cloudv2-ping-monitoring.py`, `cloudv2_fixture_simulator.py`, `cloudv2-config-ui.py`.

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
python backend/run_monitor.py
```

Por padrao o hot reload de desenvolvimento ja vem ativo (reinicia o processo em mudanca de `.py/.html/.css/.js`).
Se quiser desativar:

```bash
$env:CLOUDV2_DEV_HOT_RELOAD="0"; python backend/run_monitor.py
```

3. Abra:

```text
http://localhost:8008/login
```

## Autenticacao e LGPD

- Todas as rotas e APIs do dashboard exigem autenticacao.
- Fluxos disponiveis:
  - `GET /login`
  - `GET /register`
  - `GET /verify-email`
  - `GET /forgot-password`
  - `GET /reset-password?token=...`
- Endpoints principais:
  - `POST /auth/register`
  - `POST /auth/login`
  - `POST /auth/logout`
  - `GET /auth/verify?token=...`
  - `POST /auth/resend-verification`
  - `POST /auth/forgot-password`
  - `POST /auth/reset-password`
  - `GET /auth/me`
- Direitos do titular:
  - `GET /account/export`
  - `POST /account/delete`

### Conta admin fixa (seed automatico)

- Ao iniciar o servidor, uma conta admin global e criada/atualizada automaticamente com e-mail ja verificado.
- Valores atuais:
  - `eduardocostar03@gmail.com`
  - senha: `31380626ESP32`
- Pode customizar por variaveis:
  - `AUTH_FIXED_ADMIN_ENABLED` (default `1`)
  - `AUTH_FIXED_ADMIN_EMAIL`
  - `AUTH_FIXED_ADMIN_PASSWORD`
  - `AUTH_FIXED_ADMIN_NAME`
  - `AUTH_FIXED_ADMIN_FORCE_PASSWORD` (default `1`)

### E-mail de verificacao e reset

- DEV: por padrao, links sao exibidos no console.
- PROD: configure SMTP via variaveis:
  - `AUTH_EMAIL_MODE=smtp`
  - `AUTH_SMTP_HOST`
  - `AUTH_SMTP_PORT`
  - `AUTH_SMTP_USER`
  - `AUTH_SMTP_PASSWORD`
  - `AUTH_SMTP_FROM`
  - opcional: `AUTH_SMTP_STARTTLS=1`, `AUTH_SMTP_SSL=0`

Sem `AUTH_SMTP_*` validos, o sistema permanece em `AUTH_EMAIL_MODE=console` e nao envia e-mail real.
Se apenas `AUTH_SMTP_USER`/`AUTH_SMTP_PASSWORD` forem definidos, o backend tenta inferir host por provedor:
- Gmail: `smtp.gmail.com`
- Outlook/Hotmail/Live: `smtp.office365.com`
- Yahoo: `smtp.mail.yahoo.com`

### Seguranca

- Senha com hash `scrypt` (nunca em texto puro).
- Tokens de verificacao/reset armazenados apenas em hash.
- Sessao por cookie `HttpOnly` com `SameSite=Lax` e `Secure` quando HTTPS.
- Rate limit basico aplicado em login/reenvio/esqueci senha.

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
python backend/run_fixture_simulator.py
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

## Compatibilidade

- Wrappers legados continuam funcionando:
  - `python cloudv2-ping-monitoring.py`
  - `python cloudv2_fixture_simulator.py`
  - `python cloudv2-config-ui.py`
- Variável opcional:
  - `CLOUDV2_WEB_DIR` para forçar o diretório do frontend servido pelo backend.

## Deploy no Render (quick start)

1. Configure o servico `Web Service` com:
   - Build command: `pip install -r requirements.txt`
   - Start command: `python backend/run_monitor.py`
   - Health check path: `/login`
2. Configure os secrets:
   - `CA_CERT_CONTENT`
   - `CLIENT_CERT_CONTENT`
   - `CLIENT_KEY_CONTENT`
3. Configure variaveis de runtime (opcionais):
   - `BROKER` (default ja vem no `cloudv2-config.json`)
   - `MQTT_PORT` (porta MQTT, default `8883`)
   - `DASHBOARD_PORT` (se quiser forcar; no Render usa `PORT` automaticamente)
   - `CLOUDV2_DEV_HOT_RELOAD=0` (recomendado em producao)

Notas:
- Em Render, `PORT` e reservado para HTTP do dashboard.
- Para mudar a porta MQTT por variavel de ambiente, use `MQTT_PORT`.

## Split deploy: backend AWS + frontend Render

Sim, a estrutura atual permite separar:
- backend (MQTT + SQLite + API/auth) rodando 24/7 em VM AWS;
- frontend estatico no Render consumindo API do backend.

### 1) Backend na AWS (VM)

- Rode o monitor como servico (ex.: systemd) com `python backend/run_monitor.py`.
- Publique o HTTP com HTTPS (Nginx/Caddy + dominio), por exemplo `https://api.seudominio.com`.
- Configure variaveis importantes:
  - `CORS_ALLOWED_ORIGINS=https://seu-frontend.onrender.com`
  - `AUTH_COOKIE_SAMESITE=None`
  - `AUTH_COOKIE_SECURE=1`
  - `AUTH_BASE_URL=https://api.seudominio.com`
  - `CLOUDV2_DEV_HOT_RELOAD=0`

### 2) Frontend no Render

- Publique apenas a pasta `frontend/` como site estatico.
- Configure `frontend/runtime-config.js`:

```js
window.CLOUDV2_API_BASE_URL = "https://api.seudominio.com";
```

### 3) Observacoes de autenticacao

- Em split de dominio (Render <> AWS), login usa cookie cross-site:
  - frontend envia `credentials: include`;
  - backend responde com CORS restrito e cookie `SameSite=None; Secure`.
- Se quiser manter tudo em mesma origem (sem CORS/cookie cross-site), use proxy reverso para servir frontend e API no mesmo dominio.

## Deploy mais simples na AWS com Docker (recomendado)

### 1) Setup inicial na EC2 (uma vez)

```bash
git clone https://github.com/Monitoramento-de-Conectividade/cloud-monitoring.git
cd cloud-monitoring
bash scripts/ec2-install-docker.sh
cp .env.backend.example .env.backend
mkdir -p certs logs_mqtt
```

Copie os certificados para `certs/`:
- `certs/amazon_ca.pem`
- `certs/device.pem.crt`
- `certs/private.pem.key`

Edite `.env.backend` com seus dados reais.

### 2) Subir backend 24/7

```bash
docker compose up -d --build backend
docker compose ps
```

O container usa `restart: unless-stopped`, então volta sozinho após reboot da VM (com serviço Docker ativo).

### 3) Atualizar backend após novos commits

```bash
BRANCH=main APP_DIR=$HOME/cloud-monitoring bash scripts/ec2-deploy-backend.sh
```

### 4) Variáveis essenciais para frontend no Render

- `CORS_ALLOWED_ORIGINS=https://SEU_FRONTEND.onrender.com`
- `AUTH_COOKIE_SAMESITE=None`
- `AUTH_COOKIE_SECURE=1`
- `AUTH_BASE_URL=https://SEU_BACKEND_API`

E no frontend (`frontend/runtime-config.js`):

```js
window.CLOUDV2_API_BASE_URL = "https://SEU_BACKEND_API";
```

Importante: para cookie cross-site funcionar no login, a API da AWS deve estar em HTTPS.

## Deploy automático por commit (opcional)

Workflow incluído: `.github/workflows/deploy-aws-backend.yml`.

Secrets necessários no GitHub:
- `AWS_EC2_HOST`
- `AWS_EC2_USER`
- `AWS_EC2_SSH_KEY`
- opcional: `AWS_EC2_PORT` (default `22`)
- opcional: `AWS_EC2_APP_DIR` (default `$HOME/cloud-monitoring`)
- opcional: `AWS_REPO_URL`
