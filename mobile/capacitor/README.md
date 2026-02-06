# Android APK via Capacitor

Este diretorio prepara um app Android que abre seu dashboard hospedado (`HTTPS`) em WebView.

## Pre-requisitos

- Node.js 20+
- Android Studio (SDK + Build Tools)
- JDK 17

## Configurar URL do dashboard

1. Edite `mobile/capacitor/capacitor.config.json`.
2. Altere `server.url` para o endpoint publico:
   - Exemplo: `https://monitor.seudominio.com/index.html`

## Gerar projeto Android

1. `cd mobile/capacitor`
2. `npm install`
3. `npm run cap:add:android`
4. `npm run cap:sync`
5. `npm run cap:open`

## Gerar APK

Use o Android Studio (recomendado):

1. Abra o projeto Android via `npm run cap:open`.
2. `Build > Build Bundle(s) / APK(s) > Build APK(s)`.

Ou por linha de comando (Linux/macOS):

1. `cd mobile/capacitor/android`
2. `./gradlew assembleDebug`

No Windows (PowerShell):

1. `cd mobile/capacitor/android`
2. `.\gradlew.bat assembleDebug`

APK debug:

- `mobile/capacitor/android/app/build/outputs/apk/debug/app-debug.apk`

## Atualizar app apos mudancas

Quando alterar `capacitor.config.json`, rode:

- `npm run cap:sync`
