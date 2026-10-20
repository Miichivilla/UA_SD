# WaterManagement · SD 26/27 · Release 1

Sistema distribuido que simula la gestión centralizada del riego de los parques y jardines de una ciudad:
una **central** en la nube monitoriza y controla **estaciones de riego (WS)**, y los **operarios de campo (FO)**
solicitan riegos desde sus aplicaciones.

```
                         ┌──────────────────── CLOUD (Railway) ────────────────────┐
                         │   WM_Central ──(SQLite)                                 │
                         │      ▲  │  panel web (HTTP)        Kafka (KRaft)         │
                         └──────┼──┼─────────────────────────────▲──────▲───────────┘
                    sockets     │  │ Kafka                       │      │
              <STX>D<ETX><LRC>  │  │                             │      │
        ┌───────────────────────┘  └──────────────┐              │      │
 PC LAB 2 (WM_WS)                                 │       PC LAB 1 (WM_FO)
 ┌──────────────────────────────┐                 │       ┌───────────────────┐
 │ WM_WS_M (Monitor) ◄─sockets─►│ WM_WS_E (Engine)├Kafka──┘ WM_FO (OP-01 …)   │
 └──────────────────────────────┘                         └───────────────────┘
```

## Estructura del repositorio

| Carpeta / fichero | Contenido |
|---|---|
| `WM_Central/` | **WM_Central**: servidor de sockets concurrente (monitores), consumidor/productor Kafka, SQLite, panel web. `Dockerfile` |
| `WM_WS/` | **WM_WS_M** (Monitor) y **WM_WS_E** (Engine) de la estación de riego. `Dockerfile` |
| `WM_FO/` | **WM_FO** (aplicación del operario). `Dockerfile` |
| `kafka/` | Kafka para Railway (`Dockerfile`) y compose local |
| `lab/` | Scripts (`.sh` y `.bat`) para lanzar WS y FO con Docker en los PCs del laboratorio |
| `shared/` | Fuente única de `wm_common.py` (se copia a cada módulo con `sync_common.sh`) |
| `tests/` | Pruebas end-to-end con broker Kafka falso (no requieren Docker ni Kafka) |
| `prompts/` | Prompts de IA utilizados (requisito de la práctica) |
| `docs/` | Guía de despliegue, checklist de la guía de corrección, memoria, explicación del código |
| `Nombres.txt` | Autores del grupo |

## Cómo se ejecuta (resumen)

Guía completa paso a paso: **[`docs/DESPLIEGUE.md`](docs/DESPLIEGUE.md)**.

**Cloud (Railway):** servicios `kafka` (Root Dir `kafka/`) y `central` (Root Dir `WM_Central/`), más un volumen para la BD.

**Laboratorio (Docker):**

```bash
cp lab/lab.env.example lab/lab.env      # KAFKA=host:puerto  CENTRAL=host:puerto  (de Railway)
./lab/build.sh                          # construye wm-ws y wm-fo

# PC LAB 2 — una terminal por módulo
./lab/ws_monitor.sh WS-01               # Monitor de la estación WS-01
./lab/ws_engine.sh  WS-01               # Engine de WS-01   (tecla k = simular fuga)

# PC LAB 1
./lab/fo.sh OP-01                       # operario interactivo
./lab/fo.sh OP-02 lab/servicios_ejemplo.txt   # operario que lee los servicios de un fichero
```

**Todo en un solo PC (ensayo):** `KAFKA_HOST=host.docker.internal docker compose -f docker-compose.local.yml up -d --build`
y después los scripts de `lab/` con `KAFKA=host.docker.internal:9092` y `CENTRAL=host.docker.internal:5050`
(detalles en `docs/DESPLIEGUE.md`, apartado 6). Panel: <http://localhost:8080>.

## Parámetros (nada está fijo en el código)

| Módulo | Argumento | Variable de entorno | Defecto |
|---|---|---|---|
| **WM_Central** | `--port` puerto de sockets para los monitores | `WM_PORT` | 5050 |
| | `--kafka` host:puerto del broker | `KAFKA_BOOTSTRAP` | localhost:9092 |
| | `--http-port` panel web | `PORT` | 8080 |
| | `--db` fichero SQLite | `WM_DB` | /data/wm_central.db |
| | usuario/clave del panel | `PANEL_USER`, `PANEL_PASS` | sin clave |
| **WM_WS_M** | `--id` ID de la WS | `WS_ID` | (obligatorio) |
| | `--central` host:puerto de WM_Central | `WM_CENTRAL` | localhost:5050 |
| | `--engine-port` puerto que ofrece al Engine | `WM_ENGINE_PORT` | 6000 |
| | `--register --location "…"` dar de alta la WS | `WS_REGISTER`, `WS_LOCATION` | — |
| **WM_WS_E** | `--kafka` host:puerto del broker | `KAFKA_BOOTSTRAP` | localhost:9092 |
| | `--monitor` host:puerto de su Monitor | `WM_MONITOR` | localhost:6000 |
| | `--data-dir` estado persistente | `WM_DATA_DIR` | ./data |
| **WM_FO** | `--kafka` host:puerto del broker | `KAFKA_BOOTSTRAP` | localhost:9092 |
| | `--id` ID del operario | `FO_ID` | (obligatorio) |
| | `--file` servicios a solicitar | `FO_FILE` | — |
| | `--duration` duración por defecto (s) | `FO_DURATION` | 30 |

Datos iniciales (solo si la BD está vacía): WS `WS-01`, `WS-02`, `WS-03` (variable `WM_SEED_WS`) y operarios
`OP-01` … `OP-05`. Se pueden dar de alta más desde el panel (*Administración*) o con `--register` en el monitor.

## Estados de una WS (panel de Central)

| Estado | Color | Cuándo |
|---|---|---|
| Available | VERDE | Monitor OK y Engine OK, esperando riego |
| Watering | VERDE parpadeante + caudal L/min, volumen L, operario | Regando |
| Leak | ROJO | Engine KO (tecla `k`) o caído, con el Monitor vivo |
| Out of Service | NARANJA («Out of Service») | Bloqueada por orden de Central |
| Disconnected | GRIS | Central no recibe latidos del Monitor (y al arrancar hasta que conecten) |

## Protocolos

**Sockets** (Monitor ↔ Central y Engine ↔ Monitor): trama `<STX> DATA <ETX> <LRC>` con
`LRC = XOR byte a byte de DATA`; el receptor responde `<ACK>` o `<NACK>` (el emisor retransmite hasta 3 veces).
`DATA = OPERACIÓN#campo1#campo2…`

| Sentido | Mensajes |
|---|---|
| Monitor → Central | `AUTH#id` · `REGISTER#id#ubicación` · `HB#id#OK\|KO` (cada segundo) |
| Central → Monitor | `AUTH_OK#id#ubicación` · `AUTH_KO#motivo` · `HB_OK#estado#bloqueada#caudal#volumen#operario` |
| Engine → Monitor | `HELLO` · `READY` · respuesta a `PING`: `OK` / `KO` |
| Monitor → Engine | `ID#id#ubicación` · `WAIT#…` · `DENIED#motivo` · `PING#central_ok#bloqueada` (cada segundo) |

**Kafka** (mensajes JSON, 1 partición por topic para conservar el orden):

| Topic | Productor → Consumidor | Mensajes |
|---|---|---|
| `wm.requests` | FO / WS (riego manual) / Central → Central | `REQ` |
| `wm.ws.commands` | Central → Engine | `START` `STOP` `BLOCK` `ACTIVATE` `DENIED` |
| `wm.ws.events` | Engine → Central | `START_ACK` `START_NACK` `TELEMETRY` (1/s) `END` |
| `wm.operators` | Central → FO | `NOTIFY` (solo para el operario destinatario) |
| `wm.status` | Central → FO | `SNAPSHOT` del estado de todas las WS |

## No se pierde ninguna transacción

* Kafka: productor `acks=all` + idempotencia; Central y FO **confirman el offset después de procesar**.
* Engine: guarda el riego en disco cada segundo y el resumen final en un *outbox* persistente que solo se vacía
  cuando Kafka confirma la entrega; si cae a mitad de riego, al volver informa del estado final.
* Central: SQLite con `synchronous=FULL`; recupera de la BD los riegos que seguían activos.

## Pruebas

```bash
pip install confluent-kafka   # solo para ejecutar los módulos "de verdad"; los tests usan un stub
python3 tests/test_scenarios.py          # 11 escenarios end-to-end (caídas de Monitor, Engine, FO y Central incluidas)
```

Más: [`docs/GUIA_CORRECCION.md`](docs/GUIA_CORRECCION.md) · [`docs/MEMORIA.md`](docs/MEMORIA.md) ·
[`docs/EXPLICACION_CODIGO.md`](docs/EXPLICACION_CODIGO.md)
