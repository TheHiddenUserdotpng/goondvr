# SMB Manual (TrueNAS / NAS)

## Formaal
Denne guide forklarer hvordan du:
1. konfigurerer SMB upload i UI
2. bruger Test SMB knappen
3. forstaar SMB status/log i GoondVR

## Forudsatninger
- En SMB share paa din NAS (fx TrueNAS) med laese/skrive adgang.
- Netvaerk adgang fra GoondVR container/host til NAS paa port 445.
- En bruger med adgang til den valgte share.

## Konfiguration i Settings
Aabn Settings og udfyld sektionen `TrueNAS SMB Upload`:
- `Enable SMB Upload`: slaa upload til/fra.
- `SMB Host`: fx `192.168.1.20:445` (port 445 bruges automatisk hvis udeladt).
- `SMB Share`: share-navn (fx `media`).
- `Username` / `Password`: SMB login.
- `Domain` (valgfri): fx `WORKGROUP` eller AD-domain.
- `Remote Base Dir` (valgfri): rodmappe inde i share, fx `goondvr`.

Klik `Apply` for at gemme.

## Test SMB knap
Brug `Test SMB` i samme sektion foer du starter normal drift.

Testen goer foelgende:
1. logger ind paa SMB serveren
2. monterer den valgte share
3. opretter en midlertidig testfil
4. sletter testfilen igen

Hvis testen lykkes, er credentials/share/skriveadgang valideret.

## Upload-flow
Naar SMB er enabled, uploader GoondVR automatisk:
- Faerdige recordings (completed)
- Nye clips/highlights

Uploads koeres asynkront i baggrunden.

## Retry ved netvaerksfejl
Ved retrybare netvaerksfejl bruges retry-koe med backoff:
- flere forsoeg automatisk
- stigende ventetid mellem forsog
- jitter for at undgaa burst-retries

Permanent fejl (fx forkert share/credentials/permissions) retries ikke aggressivt som netvaerksfejl.

## UI status og log
- Pr. kanal vises SMB status badge (queued/uploading/ok/fail).
- SMB log-boksen viser samlet upload-historik og fejl live.

## Typiske fejl
- `dial ...`: host/port/netvaerk problem
- `smb auth`: forkert user/password/domain
- `mount share`: forkert share-navn eller manglende rettigheder
- `open/write/remove`: manglende skrive/slette-rettigheder paa share

## Hurtig fejlfinding
1. Verificer host, share og credentials.
2. Koer `Test SMB` igen.
3. Tjek SMB log-boksen i UI.
4. Tjek NAS-side ACL/permissions for brugeren.
5. Bekraeft at port 445 er aaben mellem GoondVR og NAS.
