## Prompt 1: pedir toda la página

```text
Quiero una página web para vigilar y controlar las estaciones de riego de mi proyecto. Se actualiza sola
cada segundo, sin tener que recargar. Todo el texto en español.

ASPECTO GENERAL
Fondo oscuro azulado, letras claras, estilo sobrio de panel de control. Debe verse bien en el ordenador
y en el móvil (en el móvil las cosas se colocan una debajo de otra).

ARRIBA DEL TODO
Una barra más oscura con el nombre "WM_Central" en negrita y al lado "WaterManagement" en gris,
centrado. A la derecha de la barra, un puntito de color con un texto corto que dice cómo va la
conexión: puntito verde y "Kafka OK" si todo va bien, rojo y "Kafka caído" si Kafka falla, y rojo con
"Sin conexión con WM_Central" si la página no consigue hablar con la Central. Al arrancar, antes de
tener datos, pone "Conectando…" en gris.

CAJA DE ÓRDENES PARA TODAS LAS ESTACIONES
Debajo de la barra, una caja con el título "Todas las WS:", un campo para escribir los segundos de
riego (por defecto 30, con "s" al lado) y tres botones: "▶ Iniciar riego" en verde, "■ Bloquear" en
naranja y "✔ Activar" en azul.

LAS ESTACIONES
Debajo, una tarjeta por cada estación, repartidas en columnas que se adaptan al ancho de la pantalla.
Si todavía no hay ninguna, mostrar el texto en gris: "No hay WS registradas. Arranca un WM_WS_M con
--location para dar de alta una."

El color de la tarjeta depende de cómo está la estación:
- Disponible: verde.
- Regando: verde más vivo que late suavemente (se ilumina y se apaga como respirando).
- Fuga: rojo.
- Fuera de servicio: naranja, con letras oscuras.
- Desconectada: gris.

Cada tarjeta tiene:
1. Arriba a la izquierda el nombre de la estación en grande y en negrita. Arriba a la derecha una
   etiqueta pequeña en mayúsculas con el estado en inglés y en español, con esta redacción exacta:
   AVAILABLE / Disponible, WATERING / Regando, LEAK / Fuga, OUT_OF_SERVICE / Fuera de servicio,
   DISCONNECTED / Desconectada.
2. Debajo, la ubicación con un icono de chincheta delante. Si no hay ubicación, una raya.
3. Si está regando, una línea que dice "Agua suministrada total:" seguida de los litros en negrita con
   2 decimales. Ese total es lo que lleva regado en toda su historia más lo que lleva el riego actual.
4. Si hay fuga, el motivo en letra cursiva y un poco más clara. Si no hay fuga, no se ve.
5. Tres cajitas más oscuras en fila, cada una con el título pequeño arriba y el valor debajo, y la
   unidad en letra aún más pequeña al lado del número:
   - Si está regando: "Caudal" (L/min, 1 decimal), "Volumen" (L, 2 decimales) y "Operario" (quién lo
     pidió; si no se sabe, una interrogación).
   - Si NO está regando: "Agua total" (L, 2 decimales), "Estado monitor" (el estado de salud, o una raya
     si no hay) y "Bloqueo" (sí o no).
   Si un valor es demasiado largo para la cajita, se corta con puntos suspensivos y al pasar el ratón
   se ve entero.
6. Si está regando, una barrita de progreso fina que se va llenando según el tiempo transcurrido sobre
   el total. Al pasar el ratón por encima dice "15/30 s" (segundos hechos / segundos totales). Si no
   está regando, no se ve.
7. Abajo del todo: un campo para los segundos (30 por defecto, más estrecho que el de arriba) y tres
   botones pequeños: "▶ Regar", "■ Bloquear" y "✔ Activar". Actúan solo sobre esa estación.

AVISO AL PULSAR UN BOTÓN
Al pulsar cualquier botón, sale un aviso pequeño flotando abajo en el centro durante unos 2 segundos:
"Orden enviada: Iniciar riego · WS01" (o "todas las WS" si fue la caja de arriba; el nombre de la acción
es Iniciar riego, Bloquear o Activar). Si la Central responde que no, dice "La Central rechazó la orden".
Si no se consigue hablar con la Central, dice "Imposible conectar con WM_Central".

ABAJO, DOS CAJAS UNA AL LADO DE LA OTRA (en el móvil, una debajo de otra)
- Izquierda: "Eventos en tiempo real". Una lista con estilo de consola (letra de ancho fijo y pequeña),
  con scroll. Cada línea empieza por la hora en gris y luego el mensaje, coloreado según su gravedad
  (rojo, naranja, amarillo, verde, azul claro o gris). El evento más reciente va ARRIBA. Si estoy
  leyendo líneas antiguas haciendo scroll, que la lista no me devuelva arriba cada segundo.
- Derecha: "Últimos riegos (BD SQLite)". Una tabla con las columnas Hora, WS, Operario, Estado, Litros
  y Motivo, también con scroll. La hora es la local del navegador. El estado "ACTIVE" va en verde y
  negrita y "INTERRUMPIDO" en rojo y negrita. El motivo se muestra con esta redacción:
  TIEMPO / Tiempo cumplido, MANUAL_WS / Finalizado en la WS, FUGA / Fuga / avería,
  BLOQUEO / Bloqueada por Central, MONITOR / Monitor caído, CENTRAL / Parada por Central,
  INTERRUMPIDO / Engine interrumpido. Si llega otro motivo, se escribe tal cual. Si no hay riegos, una
  sola fila en gris: "Sin riegos todavía". La tabla se refresca cada 2 segundos, no cada segundo.

AL FINAL DE LA PÁGINA
Centrado y en gris pequeño: "Imran y Michelle".

IMPORTANTE
No uses librerías ni cosas de internet: tiene que funcionar sin conexión. Y si un texto que llega de la
Central lleva símbolos raros (por ejemplo < o >), que se muestre tal cual y no rompa la página.
```

## Lo que añade el programador (pegar al final del prompt 1)

```text
DATOS QUE OFRECE LA CENTRAL (usar exactamente estos nombres):

La página pide cada segundo la dirección /api/state y recibe:
  {"kafka": true,
   "ws": [{"id": "WS01", "location": "Jardín", "state": "WATERING", "label": "Watering",
           "flow": 12.5, "volume": 3.4, "op": "juan", "elapsed": 15, "duration": 30,
           "total": 120.5, "reason": "", "health": "OK", "blocked": false}]}
  state puede ser AVAILABLE, WATERING, LEAK, OUT_OF_SERVICE o DISCONNECTED.
  health es "OK", "KO" o null.

También cada segundo, /api/events, que devuelve una lista (la más antigua primero):
  [{"ts": "14:03:22", "msg": "texto", "col": "green"}]
  col puede ser red, orange, yellow, green, cyan, gray o vacío.

Y cada 2 segundos, /api/waterings:
  [{"ts": 1767000000.0, "ws": "WS01", "op": "juan", "volume": 3.4, "reason": "TIEMPO", "state": "FINALIZADO"}]
  ts son segundos desde 1970. state es ACTIVE, INTERRUMPIDO o FINALIZADO.

Para las órdenes, la página envía un POST a /api/cmd con este JSON:
  {"action": "start" | "block" | "activate", "ws": "WS01" o "ALL", "duration": 30}
  Si falla, la Central responde con un error HTTP.

Las rutas son relativas (mismo servidor), sin CORS.
```

## Prompt 2: separarlo en archivos

```text
Ahora necesito que me lo des separado en tres archivos para meterlos en mi proyecto: uno solo con la
estructura de la página (panel.html), otro solo con los colores y el aspecto (panel.css) y otro solo con
lo que hace que funcione y se actualice (panel.js). El archivo de estructura tiene que enlazar a los
otros dos. Pon comentarios cortos en español para que yo pueda entender qué hace cada parte.
```

## Prompt 3: pedir cambios sueltos (ejemplos)

```text
Tengo esta página (te pego los tres archivos). No cambies nada de cómo funciona ni de los nombres de
los datos, solo quiero esto: [AQUÍ EL CAMBIO].
```

Ejemplos de cambios que se pueden pedir con palabras normales:

- "Quiero que cuando una estación tenga fuga suene un pitido corto una vez y salga un cartel rojo arriba
  con su nombre y el motivo."
- "Quiero un modo claro, con fondo blanco, y un botoncito arriba para cambiar entre claro y oscuro."
- "Quiero que los botones de una tarjeta se apaguen cuando no tiene sentido pulsarlos (no se puede
  regar si está desconectada)."
- "Quiero una gráfica pequeña del caudal de las estaciones que están regando en el último minuto."
- "Quiero poder filtrar la tabla de riegos por estación."

## Prompt 4: revisar que encaja con la Central

```text
Te pego el código de mi servidor (Python) y los tres archivos de mi página web. Revisa que los nombres de
los datos que la página lee (state, op, total, ts, volume, etc.) son exactamente los que el servidor
envía, y dime solo lo que no coincida, con el archivo y la línea. No reescribas nada que ya funcione.
```