# Checklist de la guía de corrección

Correspondencia entre cada apartado de `Practica_WaterManagement_GuiaCorreccion.pdf`, dónde está en el código y cómo
demostrarlo delante del profesor. Imprime la guía oficial a doble cara y márcala tú en la columna **E.**

## Guion de demostración (≈15 min)

| # | Acción | Qué debe verse |
|---|---|---|
| 0 | Panel de Central abierto (Railway). Mostrar repo en GitHub + acceso del profesor | Kafka en verde; `WS-01..03` **GRIS (Disconnected)** con su ubicación |
| 1 | PC LAB 2: `ws_monitor.sh WS-01` y `ws_engine.sh WS-01`; igual con `WS-02` | Monitor «Autenticada»; WS en **VERDE** |
| 2 | Monitor con ID inexistente: `ws_monitor.sh WS-99` | «AUTENTICACIÓN DENEGADA: WS no registrada» |
| 3 | Alta: `ws_monitor.sh WS-07 "Parque Nuevo"` | Aparece la WS nueva (BD) |
| 4 | PC LAB 1: `fo.sh OP-01` → opción 2 → `WS-01`, 20 s | FO: recibida → disponible → autorizada → telemetría. Panel: **VERDE parpadeante**, L/min, L, `OP-01` |
| 5 | `fo.sh OP-02 lab/servicios_ejemplo.txt` | Servicios en orden, 4 s entre cada uno, uno denegado (`WS-99`) |
| 6 | Panel: *Bloquear* una WS (y *todas*), *Iniciar riego* (una/todas), *Activar* | **NARANJA «Out of Service»**; riego cortado; vuelve a verde |
| 7 | Riego manual en la WS: tecla `r` en el Engine; `f` para finalizar | Central lo autoriza; resumen final |
| 8 | Tecla `k` en el Engine (durante un riego) | Monitor notifica; WS **ROJA**; riego termina de inmediato; FO informado. `k` otra vez → verde |
| 9 | `crash.sh WS-01 monitor` (con riego) | **GRIS**; riego termina; el Engine muestra «AVERIADA» y no admite riegos ni manuales. Relanzar monitor → recuperado sin tocar el Engine |
| 10 | `crash.sh WS-01 engine` (con riego) | **ROJO**; FO informado. Relanzar engine → envía el volumen final |
| 11 | `crash.sh OP-01 fo` durante un riego y relanzar | El riego sigue; al volver ve `[pendiente] RIEGO FINALIZADO…` |
| 12 | Parar `central` en Railway con una WS regando | WS sigue hasta acabar y «Imposible conectar con WM_Central»; FO igual; sin nuevos riegos. Redesplegar → todo se recupera solo |
| 13 | Lanzar un FO / una WS nueva a petición del profesor | Sin fallos (escalabilidad) |

## Despliegue, modularidad y escalabilidad (2 pts)

| Requisito | Dónde / cómo |
|---|---|
| Cloud (Central, Kafka, SQLite) en Railway ya desplegada | `docs/DESPLIEGUE.md` §2 · `WM_Central/Dockerfile` · `kafka/Dockerfile` |
| WM_FO y WM_WS en laboratorio, sin IDE | Docker: `lab/*.sh` / `lab/*.bat` |
| GitHub `UA_SD` con estructura común y acceso al profesor | `WM_Central/ WM_WS/ WM_FO/ prompts/ Readme.md Nombres.txt` |
| Tantas instancias como se pida, o parar una súbitamente | Cada WS = 1 monitor + 1 engine con su ID; FO con su ID; `lab/crash.sh` |

## Funcionamiento base (3 pts)

| Requisito | Dónde |
|---|---|
| Parámetros sin valores fijos; sin recompilar | Argumentos + variables de entorno (tabla del `Readme.md`); Python no se compila |
| Central muestra al (re)iniciar las WS de su BD como **Desconectadas** | `Central._load()` + `print_table()`; panel |
| Panel: ID, ubicación, agua suministrada, estado y colores | `panel.html` (`s-AVAILABLE`, `s-WATERING` parpadea, `s-LEAK`, `s-OUT_OF_SERVICE`, `s-DISCONNECTED`) |
| Registro y autenticación (aceptar/denegar WS no registrada) | `Central.authenticate()` · Monitor: `AUTH`/`REGISTER` |
| Validar disponibilidad y aceptar/denegar riego | `Central.on_request()` |
| Central → WS: iniciar, bloquear, activar (una o todas) | `cmd_start/cmd_block/cmd_activate` · botones del panel |
| Riegos desde FO, menú de la WS o Central | FO opción 2/3 · Engine tecla `r` · panel *Regar* |
| Monitor ↔ Engine cada segundo; KO por tecla → fuga | `WM_WS_M.handle_engine` (PING) · `WM_WS_E` tecla `k` |
| Fuga durante riego: finaliza y se informa a todos | `Central.on_heartbeat` · `Engine.toggle_ko` |
| Operario solo recibe los mensajes de **su** WS/petición | `notify()` → topic `wm.operators`, filtrado por `op` en FO |
| Servidores concurrentes | Central: un hilo por monitor · Monitor: un hilo por engine |
| Ninguna transacción se pierde | Offsets confirmados tras procesar, outbox del Engine, SQLite `synchronous=FULL` |

## Resiliencia (3 pts, solo si el funcionamiento base es correcto)

| Caso | Comportamiento implementado | Tabla de estados |
|---|---|---|
| Cae un **Monitor** | Central → *Desconectada*; riego finaliza; la WS muestra «AVERIADA» y rechaza riegos (también manuales) | Monitor KO + Engine OK/KO ⇒ Desconectado |
| Cae un **Engine** | Monitor avisa → *Leak*; FO informado; al volver el Engine envía el estado final (lo regado no se pierde) | Monitor OK + Engine KO ⇒ Averiado (rojo) |
| Se cierra un **FO** en pleno servicio | El riego sigue; al volver ve el resultado (`[pendiente]`) | — |
| Cae **Central** | Las WS terminan el riego y no admiten nuevos; «Imposible conectar con WM_Central» en FO/WS/Monitor | — |
| Recuperación | Sin reiniciar el resto de módulos; altas de nuevas instancias sin fallos | — |

Todos estos casos están cubiertos por `tests/test_scenarios.py` (`moncrash`, `engine`, `fo`, `central`, …).

## General (2 pts)

* **Memoria:** `docs/MEMORIA.md` (plantilla con el contenido técnico ya redactado; añade portada y capturas).
* **Extras reseñables:** tramas `<STX>D<ETX><LRC>` con ACK/NACK y retransmisión · outbox persistente · antigüedad de
  mensajes con la marca de tiempo **del broker** (no depende de relojes sincronizados) · panel web con usuario/clave ·
  batería de 11 pruebas end-to-end · scripts para simular caídas.
* **Uso responsable de la IA:** `prompts/prompts.md` completo y **sé capaz de explicar el código** → `docs/EXPLICACION_CODIGO.md`.
