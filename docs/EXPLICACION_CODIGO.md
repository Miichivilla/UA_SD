# Explicación del código (para la defensa)

El profesor puede pedir explicaciones de cualquier línea. Este documento resume **qué hace cada pieza y por qué**.
Léelo con el código delante; si algo no lo entiendes, pregunta antes de la corrección.

## 1. Idea general
* **Sockets** donde se necesita una conexión directa y ligera, con detección inmediata de caída: Monitor→Central
  (latido) y Engine↔Monitor (salud).
* **Kafka** donde se quiere desacoplar y no perder mensajes: peticiones de riego, órdenes a las WS, telemetría,
  notificaciones al operario. Si un módulo cae, los mensajes esperan en el topic.
* **SQLite** solo en Central: WS, operarios, historial de riegos. Es la memoria que sobrevive a un reinicio.

## 2. `wm_common.py` (idéntico en los 3 módulos)
* `lrc()`/`build_frame()`: trama `STX + datos + ETX + LRC`; LRC = XOR de los bytes de los datos.
* `FramedConn.send()` espera ACK (si NACK, reenvía, 3 veces). `recv()` valida el LRC y contesta ACK/NACK.
  El diálogo siempre es petición→respuesta, por eso el byte siguiente a una trama enviada es su ACK/NACK.
* `KafkaBus`: productor `acks=all` + idempotente; `send_sync` espera confirmación del broker; consumidores con
  grupo y **commit manual después de procesar** (si se cae a mitad, el mensaje se reentrega: *at-least-once*).
* Topics con `LogAppendTime`: la hora del mensaje la pone el **broker**, así `msg_age()` mide la antigüedad
  sin fiarse de que los relojes de los PCs coincidan.

## 3. WM_Central
* `serve()` acepta conexiones y lanza **un hilo por monitor** (`serve_monitor`) → concurrente.
* Protocolo: `AUTH`/`REGISTER` → `AUTH_OK|AUTH_KO`; después `HB#id#OK|KO` cada segundo.
* `WS.state` deriva el estado: sin monitor ⇒ DISCONNECTED; salud KO ⇒ LEAK; bloqueada ⇒ OUT_OF_SERVICE;
  riego ⇒ WATERING; si no AVAILABLE. (Orden = prioridad.)
* `kafka_loop` consume `wm.requests` y `wm.ws.events` y **hace commit tras procesar**.
* `on_request`: valida operario, existencia y estado de la WS; si todo OK reserva (`pending`) y manda `START`;
  el riego solo se da por iniciado cuando el Engine contesta `START_ACK`.
* `reaper_loop`: latido caducado (5 s) ⇒ Desconectada; `START` sin respuesta (6 s) ⇒ denegado; telemetría perdida
  (12 s) ⇒ cierra el riego.
* `_on_end`: si Central ya había cerrado el riego (fuga/bloqueo) y luego llega el `END` exacto del Engine, **corrige**
  el volumen en BD (`set_session_volume_final`).
* Al arrancar: `_load()` lee la BD, deja todas las WS en Desconectada y recupera riegos activos.

## 4. WM_WS_M (Monitor)
* `central_loop`: se autentica (o `--register`), envía `HB` cada segundo; si falla, «Imposible conectar con
  WM_Central» y reintenta cada 2 s.
* `engine_server`/`handle_engine`: servidor para el Engine; le da su ID (`ID#…`), y cada segundo `PING`.
  Si el Engine no responde en 1,5 s o dice KO ⇒ `health = KO` ⇒ el siguiente `HB` lleva KO ⇒ fuga en Central.
* El `PING` lleva si Central está viva y si la WS está bloqueada: así el Engine se entera sin hablar con Central.

## 5. WM_WS_E (Engine)
* Obtiene su ID del Monitor (validado por Central). Órdenes por Kafka con consumidor «tail» (solo lo nuevo → no
  ejecuta `START` antiguos).
* `tick_loop`: cada segundo calcula caudal aleatorio, acumula volumen, **guarda el riego en disco** y publica `TELEMETRY`.
* `end_session`: el `END` va a un **outbox persistente** (`outbox_loop` lo entrega y lo borra solo con confirmación).
* Si el proceso cae a mitad de riego, `identify()` encuentra el fichero de sesión y envía un `END` con razón
  `INTERRUMPIDO` y el volumen guardado.
* Sin Monitor ⇒ «AVERIADA» y rechaza todo; sin Central ⇒ termina el riego en curso y rechaza nuevos.
* Teclado: con terminal basta **una tecla** (`read_key`, modo cbreak); sin terminal lee líneas.

## 6. WM_FO
* `notif_loop`: consumidor con grupo `wm-fo-<ID>` de `wm.operators` ⇒ recuerda su offset; lo que ocurrió con el FO
  cerrado se muestra con `[pendiente]`. Solo se muestran los mensajes con `op == <mi ID>`.
* `status_loop` lee la «foto» de `wm.status`; si deja de llegar ⇒ «Imposible conectar con WM_Central».
* `run_file`: envía un servicio, espera la conclusión (FINAL o DENIED), 4 s, y el siguiente.

## 7. Preguntas típicas
| Pregunta | Respuesta corta |
|---|---|
| ¿Por qué sockets *y* Kafka? | Latido y salud necesitan conexión directa y detectar la caída al instante; el resto se beneficia del desacoplamiento y la persistencia de Kafka |
| ¿Cómo evitas perder un riego si Central cae? | El Engine sigue; su `END` queda en Kafka (y en su outbox); Central lo procesa al volver |
| ¿Y duplicados? | Productor idempotente; `on_request` ignora `req_id` ya existentes; `END` es idempotente |
| ¿Qué es el LRC y para qué? | XOR de los bytes del mensaje; detecta corrupción; el receptor contesta NACK y el emisor retransmite |
| ¿Cómo sabe Central que un Monitor murió? | Se cierra el socket o pasan 5 s sin `HB` |
| ¿Diferencia Monitor caído / Engine caído? | Monitor caído ⇒ Central no recibe latido ⇒ Desconectada (gris). Engine caído ⇒ el Monitor sí envía `HB#KO` ⇒ Leak (rojo) |
| ¿Por qué 1 partición por topic? | Conserva el orden de los eventos (p. ej. `START` antes que `STOP`) |
| ¿Qué pasa si dos FO piden la misma WS? | La primera reserva la WS (`pending`/`session`); la segunda recibe «WS ocupada» (secciones críticas con `self.lock`) |
| ¿Dónde se guarda la BD en Railway? | En un volumen montado en `/data` |
