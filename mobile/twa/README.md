# Android APK via TWA (Bubblewrap)

Use esta opcao quando voce quiser que o app Android rode diretamente seu PWA com experiencia de navegador confiavel.

## Pre-requisitos

- Dashboard disponivel em `HTTPS` e dominio proprio.
- Arquivo `assetlinks.json` publicado em:
  - `https://SEU_DOMINIO/.well-known/assetlinks.json`
- Node.js 20+
- JDK 17
- Android SDK (build-tools + platform-tools)

## Passos

1. Instale o Bubblewrap:
   - `npm install -g @bubblewrap/cli`
2. Use o template:
   - `mobile/twa/twa-manifest.template.json`
3. Inicialize o projeto TWA:
   - `bubblewrap init --manifest https://SEU_DOMINIO/manifest.json`
4. Gere o APK:
   - `bubblewrap build`

## Arquivos de apoio

- Template da configuracao TWA:
  - `mobile/twa/twa-manifest.template.json`
- Template de associacao de dominio:
  - `mobile/twa/assetlinks.template.json`

## Observacoes

- Sem `assetlinks.json` valido, o app ainda abre, mas pode usar fallback de Custom Tabs.
- Para Play Store, use assinatura release e versao de app gerenciadas no projeto TWA gerado.
