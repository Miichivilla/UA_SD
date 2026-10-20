# Guía de despliegue paso a paso

Escenario exigido: **Cloud (Railway)** = WM_Central + Kafka + SQLite · **PC LAB 1** = WM_FO · **PC LAB 2** = WM_WS (Monitor + Engine).
Docker es obligatorio para todos los módulos. Ciclo CI/CD: **VS Code → GitHub → Railway**.

> ⚠️ El día de la corrección hay que llegar con la parte Cloud **ya desplegada y funcionando**. Ensáyalo al menos una vez
> completo (también el apartado 6, «ensayo en un solo PC»).

---
## 1. GitHub (repositorio `UA_SD`)

1. En VS Code abre la carpeta del proyecto → *Source Control* → **Initialize Repository** → commit → **Publish Branch** → repo
   `UA_SD` (privado). O por consola:
   ```bash
   git init && git add . && git commit -m "WaterManagement R1"
   git branch -M main && git remote add origin https://github.com/<usuario>/UA_SD.git && git push -u origin main
   ```
2. **Dar acceso al profesor:** GitHub → repo → *Settings* → *Collaborators* → *Add people* → usuario del profesor.
3. Comprueba la estructura común: `WM_Central/ WM_WS/ WM_FO/ prompts/ Readme.md Nombres.txt` (rellena `Nombres.txt` y
   añade tus prompts anteriores a `prompts/prompts.md`).

## 2. Cloud en Railway

Crea un proyecto: *New Project → Empty Project*. Añadirás **dos servicios** desde el mismo repo (*+ New → GitHub Repo → UA_SD*).

### 2.1 Servicio `kafka`
1. *Settings → Source → Root Directory* = `kafka` (Railway detecta el `Dockerfile`). Nombra el servicio **kafka**.
2. *Settings → Volumes (o `Ctrl+K` → Volume)* → crear volumen con *Mount Path* `/var/lib/kafka/data` (los datos sobreviven a los redespliegues).
3. *Settings → Networking → TCP Proxy → + Add* → puerto **9092**. Railway te da `xxxxx.proxy.rlwy.net:19097`.
4. *Variables* del servicio `kafka`:
   | Variable | Valor |
   |---|---|
   | `KAFKA_PUBLIC_HOST` | `xxxxx.proxy.rlwy.net` (host del proxy) |
   | `KAFKA_PUBLIC_PORT` | `19097` (puerto del proxy) |
5. Redespliega. En los *Deploy Logs* debe aparecer `[wm-kafka] externo : xxxxx.proxy.rlwy.net:19097` y el broker arrancado.
6. **Prueba desde tu PC** (igual que el tutorial de Railway), con `pip install confluent-kafka`:
   ```bash
   python -c "from confluent_kafka.admin import AdminClient as A; print(list(A({'bootstrap.servers':'xxxxx.proxy.rlwy.net:19097'}).list_topics(timeout=10).topics))"
   ```
   Si lista topics (aunque sea vacío) sin error, Kafka es accesible desde fuera.

> *Alternativa (tutorial de clase):* plantilla `apache/kafka` de Railway + TCP Proxy 9092. Funciona igual de cara a los
> equipos del laboratorio, pero el broker anuncia una sola dirección: en ese caso pon también la **dirección pública**
> del proxy en `KAFKA_BOOTSTRAP` de Central.

### 2.2 Servicio `central`
1. *+ New → GitHub Repo → UA_SD*, *Root Directory* = `WM_Central`. Nombra el servicio **central**.
2. Volumen con *Mount Path* `/data` (aquí vive la BD **SQLite**: sin volumen se perdería en cada redespliegue).
3. *Variables*:
   | Variable | Valor |
   |---|---|
   | `KAFKA_BOOTSTRAP` | `${{kafka.RAILWAY_PRIVATE_DOMAIN}}:29092` (red privada). Si falla: la dirección pública del proxy |
   | `WM_PORT` | `5050` |
   | `WM_DB` | `/data/wm_central.db` |
   | `PANEL_USER` / `PANEL_PASS` | usuario y clave del panel web |
4. *Networking*:
   * **Generate Domain** → URL pública del **panel web** (Railway enruta a la variable `PORT`).
   * **TCP Proxy → + Add → puerto 5050** → `yyyyy.proxy.rlwy.net:25050`: es el `CENTRAL` que usarán los **monitores** del laboratorio.
5. Abre la URL del panel (te pedirá usuario/clave): debe verse **Kafka en verde** y `WS-01…WS-03` en **gris (Disconnected)** con su ubicación.

### 2.3 CI/CD
Cualquier `git push` a `main` reconstruye y redespliega los servicios afectados (pestaña *Deployments*). Para demostrarlo:
cambia un texto del panel, `git commit` + *Sync Changes*, y mira cómo aparece un despliegue nuevo.

## 3. Laboratorio (PC LAB 1 y PC LAB 2)

Requisitos: Docker Desktop en marcha, `git`, salida a Internet a los puertos de los TCP Proxy de Railway.

```bash
git clone https://github.com/<usuario>/UA_SD.git && cd UA_SD
cp lab/lab.env.example lab/lab.env       # (Windows: copy lab\lab.env.example lab\lab.env)
#   edita lab/lab.env:  KAFKA=xxxxx.proxy.rlwy.net:19097   CENTRAL=yyyyy.proxy.rlwy.net:25050
./lab/build.sh                           # Windows: lab\build.bat
```

### PC LAB 2 — Watering Stations (una terminal por módulo)
```bash
./lab/ws_monitor.sh WS-01     # terminal 1: Monitor de WS-01 (se autentica en Central, latido cada 1 s)
./lab/ws_engine.sh  WS-01     # terminal 2: Engine de WS-01   · teclas: r regar · f finalizar · k KO · s estado · q salir
./lab/ws_monitor.sh WS-02     # terminal 3
./lab/ws_engine.sh  WS-02     # terminal 4
```
Una WS nueva se da de alta con `./lab/ws_monitor.sh WS-07 "Parque Nuevo"` (envía `REGISTER`).
Cada Engine usa un volumen Docker (`wm-ws-01-data`) para guardar el estado del riego aunque el contenedor «muera».

### PC LAB 1 — Field Operators
```bash
./lab/fo.sh OP-01                                 # interactivo: 1 ver estaciones · 2 solicitar riego · 3 fichero · 0 salir
./lab/fo.sh OP-02 lab/servicios_ejemplo.txt       # lee los servicios de un fichero (espera 4 s entre cada uno)
```
Formato del fichero: una línea por servicio, `ID_WS#SEGUNDOS` (segundos opcional); `#` al inicio = comentario.

### Comprobación
En el panel de Central `WS-01` y `WS-02` pasan a **verde**; al pedir un riego desde `OP-01`, la tarjeta **parpadea** con caudal,
volumen y operario, y el FO ve cada paso (recibida → disponible → autorizada → telemetría → resumen final).

## 4. Detener / simular caídas súbitas
```bash
./lab/crash.sh WS-01 monitor    # docker kill del monitor     ·   ./lab/crash.sh WS-01 engine   ·   ./lab/crash.sh OP-01 fo
```
Para caer **Central**: en Railway, servicio `central` → *⋯ → Remove deployment* (o *Restart*); para volver, *Redeploy*.

## 5. Problemas frecuentes
| Síntoma | Causa / solución |
|---|---|
| FO/Engine: «Imposible conectar con Kafka» | `KAFKA` en `lab.env` mal, o `KAFKA_PUBLIC_HOST/PORT` de Railway no coinciden con el TCP Proxy. Reprueba con el comando del 2.1 |
| Monitor: «Imposible conectar con WM_Central» | `CENTRAL` apunta al proxy del puerto **5050** (no al dominio web); Central desplegado |
| Panel con Kafka en rojo | `KAFKA_BOOTSTRAP` de `central` incorrecto → usa la dirección pública del proxy |
| La BD se vacía tras redesplegar | Falta el volumen en `/data` del servicio `central` |
| Se pierde el estado del Engine | Lanza el Engine con `ws_engine.sh` (monta el volumen) |
| Firewall del laboratorio | Debe permitir conexiones salientes a los puertos de `*.proxy.rlwy.net` |

## 6. Ensayo en un solo PC (antes de tocar Railway)
El broker **anuncia** al cliente la dirección a la que debe reconectarse, así que `KAFKA_HOST` debe ser la misma que
uses en `lab.env` (si anunciara `localhost`, un contenedor se reconectaría a sí mismo y fallaría).

```bash
# Docker Desktop (Windows/macOS): los contenedores ven el PC como host.docker.internal
KAFKA_HOST=host.docker.internal docker compose -f docker-compose.local.yml up -d --build   # Kafka + Central + SQLite
cp lab/lab.env.example lab/lab.env
#   lab.env:  KAFKA=host.docker.internal:9092   CENTRAL=host.docker.internal:5050
./lab/build.sh && ./lab/ws_monitor.sh WS-01     # etc. Panel en http://localhost:8080 (admin / admin)
```
En Linux sin Docker Desktop usa la IP del PC (`hostname -I`) en `KAFKA_HOST`, `KAFKA` y `CENTRAL`.
(PowerShell: `$env:KAFKA_HOST="host.docker.internal"; docker compose -f docker-compose.local.yml up -d --build`.)
