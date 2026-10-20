#!/usr/bin/env python3
"""
Escenarios de prueba end-to-end (procesos reales + broker Kafka falso).
Cada escenario verifica un bloque de la guía de corrección.

  python3 tests/test_scenarios.py            # todos
  python3 tests/test_scenarios.py leak block # sólo algunos
"""
import os
import re
import sqlite3
import sys
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness import Env

RESULTS = []


def check(cond, msg):
    if not cond:
        raise AssertionError(msg)
    print(f"    ✔ {msg}")


def boot(tag, port, ws=("WS-01",), fos=(), wait_kafka=True):
    e = Env(tag, port)
    e.broker()
    time.sleep(0.5)
    c = e.central()
    c.wait_for("Conectado a Kafka", 15)
    mons, engs = {}, {}
    for w in ws:
        mons[w], p = e.monitor(w)
        engs[w] = e.engine(w, p)
    for w in ws:
        e.wait_state(w, "AVAILABLE", 20)
    fo = {}
    for o in fos:
        fo[o] = e.fo(o)
        fo[o].wait_for("Conectado a Kafka")
    if fos:
        time.sleep(2.5)           # primer snapshot de estado
    return e, c, mons, engs, fo


def db_rows(e, sql, *a):
    con = sqlite3.connect(e.dbfile)
    con.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in con.execute(sql, a)]
    finally:
        con.close()


# ---------------------------------------------------------------------------
def t_basic():
    """Despliegue, registro/autenticación, denegaciones, filtrado por operario."""
    e, c, mons, engs, fo = boot("basic", 21100, ("WS-01",), ("OP-01", "OP-02"))
    try:
        c.wait_for(r"WS WS-01 AUTENTICADA")
        # WS no registrada -> denegada
        m99, _ = e.monitor("WS-99")
        m99.wait_for("AUTENTICACIÓN DENEGADA", 10)
        time.sleep(1.5)
        check(not m99.alive(), "monitor de WS no registrada: denegado y termina")
        check(c.has("Autenticación DENEGADA"), "Central registra la denegación")
        # Alta con --register
        m77, _ = e.monitor("WS-77", register="Parque Nuevo")
        e.wait_state("WS-77", "LEAK", 10)       # sin engine -> avería (Monitor OK + Engine KO)
        check(e.ws_state("WS-77")["location"] == "Parque Nuevo", "alta de una WS nueva con ubicación")
        check(any(r["id"] == "WS-77" for r in db_rows(e, "select id from watering_stations")), "WS-77 persistida en SQLite")
        # WS desconectada y operario desconocido
        s = fo["OP-01"].mark()
        fo["OP-01"].send("2 WS-02 5")
        fo["OP-01"].wait_for("DENEGADA: WS desconectada", 10, s)
        check(True, "petición a WS desconectada -> DENEGADA")
        fx = e.fo("OP-99"); fx.wait_for("Conectado a Kafka"); time.sleep(2.5)
        fx.send("2 WS-01 5"); fx.wait_for("no registrado", 10)
        check(True, "operario no registrado -> DENEGADA")
        # riego + segundo operario
        fo["OP-01"].send("2 WS-01 8")
        fo["OP-01"].wait_for("RIEGO AUTORIZADO", 10)
        w = e.wait_state("WS-01", "WATERING", 10)
        time.sleep(2)
        w = e.ws_state("WS-01")
        check(w["operator"] == "OP-01" and w["flow"] > 0 and w["volume"] > 0, f"panel: Watering con caudal {w['flow']} L/min, vol {w['volume']} L, op OP-01")
        s2 = fo["OP-02"].mark()
        fo["OP-02"].send("2 WS-01 5")
        fo["OP-02"].wait_for("ocupada", 10, s2)
        check(True, "segunda petición a WS ocupada -> DENEGADA")
        fo["OP-01"].wait_for("RIEGO FINALIZADO", 15)
        check(not fo["OP-02"].has("Regando en WS-01"), "OP-02 NO recibe los mensajes del riego de OP-01")
        time.sleep(1)
        rows = db_rows(e, "select * from sessions where status='ENDED'")
        check(len(rows) == 1 and rows[0]["volume"] > 0.5, f"riego guardado en BD ({rows[0]['volume']:.2f} L)")
        tot = db_rows(e, "select total_liters t from watering_stations where id='WS-01'")[0]["t"]
        check(abs(tot - rows[0]["volume"]) < 0.01, "agua total acumulada de la WS en BD")
    finally:
        e.stop_all()


def t_leak():
    """Fuga: engine KO -> rojo, fin inmediato del riego, recuperación."""
    e, c, mons, engs, fo = boot("leak", 21200, ("WS-01",), ("OP-01",))
    try:
        fo["OP-01"].send("2 WS-01 40")
        fo["OP-01"].wait_for("RIEGO AUTORIZADO", 10)
        e.wait_state("WS-01", "WATERING", 10)
        time.sleep(3)
        engs["WS-01"].send("k")
        e.wait_state("WS-01", "LEAK", 8)
        check(True, "engine KO -> Central muestra LEAK (rojo)")
        fo["OP-01"].wait_for("fuga / avería detectada", 5)
        check(True, "el riego finaliza inmediatamente y se informa al operario")
        check(engs["WS-01"].has("RIEGO FINALIZADO \\(fuga"), "el engine cierra la electroválvula")
        check(not engs["WS-01"].has("Resumen final del riego .* entregado"), "durante el KO el engine retiene el resumen final")
        s = fo["OP-01"].mark()
        fo["OP-01"].send("2 WS-01 5")
        fo["OP-01"].wait_for("DENEGADA: WS con fuga", 10, s)
        check(True, "petición a WS con fuga -> DENEGADA")
        engs["WS-01"].send("r 5")
        engs["WS-01"].wait_for("No se puede regar: WS con fuga", 5)
        check(True, "riego manual rechazado en la propia WS durante la fuga")
        engs["WS-01"].send("k")
        e.wait_state("WS-01", "AVAILABLE", 8)
        check(True, "avería resuelta -> vuelve a AVAILABLE")
        engs["WS-01"].wait_for("Resumen final del riego .* entregado", 8)
        check(True, "al recuperarse, el engine envía el estado final del riego")
    finally:
        e.stop_all()


def t_block():
    """Central: bloquear / activar una o todas, y riego desde Central."""
    e, c, mons, engs, fo = boot("block", 21300, ("WS-01", "WS-02"), ("OP-01",))
    try:
        fo["OP-01"].send("2 WS-01 40")
        fo["OP-01"].wait_for("RIEGO AUTORIZADO", 10)
        e.wait_state("WS-01", "WATERING", 10)
        time.sleep(2)
        e.api("/api/cmd", {"cmd": "block", "ws": "WS-01"})
        e.wait_state("WS-01", "OUT_OF_SERVICE", 5)
        check(True, "bloquear WS en riego -> OUT_OF_SERVICE (naranja)")
        fo["OP-01"].wait_for("bloqueada por Central", 5)
        check(True, "el riego en curso se finaliza y se informa al operario")
        engs["WS-01"].wait_for("OUT OF SERVICE", 5)
        engs["WS-01"].send("r 5")
        engs["WS-01"].wait_for("No se puede regar: WS fuera de servicio", 5)
        check(True, "la WS bloqueada rechaza el riego manual")
        s = fo["OP-01"].mark()
        fo["OP-01"].send("2 WS-01 5")
        fo["OP-01"].wait_for("DENEGADA: WS fuera de servicio", 10, s)
        check(True, "petición a WS bloqueada -> DENEGADA")
        e.api("/api/cmd", {"cmd": "activate", "ws": "WS-01"})
        e.wait_state("WS-01", "AVAILABLE", 5)
        check(True, "activar -> vuelve a AVAILABLE (verde)")
        e.api("/api/cmd", {"cmd": "block", "ws": "ALL"})
        e.wait_state("WS-01", "OUT_OF_SERVICE", 5); e.wait_state("WS-02", "OUT_OF_SERVICE", 5)
        check(True, "bloquear TODAS las WS")
        e.api("/api/cmd", {"cmd": "activate", "ws": "ALL"})
        e.wait_state("WS-01", "AVAILABLE", 5); e.wait_state("WS-02", "AVAILABLE", 5)
        e.api("/api/cmd", {"cmd": "start", "ws": "ALL", "duration": 6})
        e.wait_state("WS-01", "WATERING", 8); e.wait_state("WS-02", "WATERING", 8)
        check(True, "iniciar riego en TODAS las WS desde Central")
        e.wait_state("WS-01", "AVAILABLE", 15); e.wait_state("WS-02", "AVAILABLE", 15)
        e.api("/api/cmd", {"cmd": "start", "ws": "WS-02", "duration": 5})
        e.wait_state("WS-02", "WATERING", 8)
        check(True, "iniciar riego en UNA WS desde Central")
    finally:
        e.stop_all()


def t_monitor_crash():
    """Cae un Monitor: Desconectada, riego finalizado, WS averiada; recuperación sin reiniciar el engine."""
    e, c, mons, engs, fo = boot("moncrash", 21400, ("WS-01", "WS-02"), ("OP-01",))
    try:
        fo["OP-01"].send("2 WS-01 40")
        fo["OP-01"].wait_for("RIEGO AUTORIZADO", 10)
        e.wait_state("WS-01", "WATERING", 10)
        mons["WS-01"].kill()
        e.wait_state("WS-01", "DISCONNECTED", 8)
        check(True, "monitor caído -> Central marca DISCONNECTED (gris)")
        check(e.ws_state("WS-02")["state"] == "AVAILABLE", "el resto de WS sigue operativa")
        fo["OP-01"].wait_for("monitor de la WS desconectado", 5)
        check(True, "el riego finaliza y se informa al operario")
        engs["WS-01"].wait_for("AVERIADA", 8)
        engs["WS-01"].send("r 5")
        engs["WS-01"].wait_for("No se puede regar: WS averiada", 5)
        check(True, "la WS muestra 'Averiada' y no admite riegos ni manuales")
    finally:
        e.stop_all()


def t_monitor_restart():
    """Reinicio del monitor con el mismo puerto: engine se reengancha sin reiniciarse."""
    e, c, mons, engs, fo = boot("monrestart", 21450, ("WS-01",), ("OP-01",))
    try:
        port = e.next_engine_port
        mons["WS-01"].kill()
        e.wait_state("WS-01", "DISCONNECTED", 8)
        engs["WS-01"].wait_for("Monitor perdido", 8)
        m, _ = e.monitor("WS-01", port=port, name="WS-01_M2")
        e.wait_state("WS-01", "AVAILABLE", 20)
        check(True, "al reiniciar SÓLO el monitor, el engine se reconecta y la WS vuelve a AVAILABLE")
        check(engs["WS-01"].alive(), "el engine no se ha reiniciado")
        fo["OP-01"].send("2 WS-01 5")
        fo["OP-01"].wait_for("RIEGO FINALIZADO", 15)
        check(True, "riego correcto tras la recuperación")
    finally:
        e.stop_all()


def t_engine_crash():
    """Cae el engine durante un riego: avería, y al volver informa del estado final."""
    e, c, mons, engs, fo = boot("engcrash", 21500, ("WS-01",), ("OP-01",))
    try:
        fo["OP-01"].send("2 WS-01 40")
        fo["OP-01"].wait_for("RIEGO AUTORIZADO", 10)
        e.wait_state("WS-01", "WATERING", 10)
        time.sleep(4)
        port = e.next_engine_port
        engs["WS-01"].kill()
        e.wait_state("WS-01", "LEAK", 8)
        check(True, "engine caído -> el monitor avisa y Central marca la WS como averiada (LEAK)")
        fo["OP-01"].wait_for("RIEGO FINALIZADO en WS-01", 8)
        check(True, "se informa al operario del fin del riego")
        before = db_rows(e, "select volume v from sessions")[0]["v"]
        en2 = e.engine("WS-01", port, name="WS-01_E2")
        en2.wait_for("Riego interrumpido recuperado", 15)
        en2.wait_for("Resumen final del riego .* entregado", 10)
        check(True, "al volver, el engine recupera del disco el riego y envía el estado final")
        e.wait_state("WS-01", "AVAILABLE", 10)
        check(True, "tras reiniciar sólo el engine, la WS vuelve a AVAILABLE")
        after = db_rows(e, "select volume v, reason r from sessions")[0]
        check(after["v"] >= before, f"no se pierde lo regado ({before:.2f} -> {after['v']:.2f} L)")
    finally:
        e.stop_all()


def t_fo_crash():
    """Se cierra un FO durante el riego: el riego sigue y al volver ve el resultado."""
    e, c, mons, engs, fo = boot("focrash", 21600, ("WS-01",), ("OP-01",))
    try:
        fo["OP-01"].send("2 WS-01 8")
        fo["OP-01"].wait_for("RIEGO AUTORIZADO", 10)
        time.sleep(2)
        fo["OP-01"].kill()
        engs["WS-01"].wait_for("RIEGO FINALIZADO", 15)
        check(True, "el riego sigue su curso con el FO cerrado")
        e.wait_state("WS-01", "AVAILABLE", 8)
        fo2 = e.fo("OP-01", name="FO_OP-01_b")
        fo2.wait_for("\\[pendiente\\] RIEGO FINALIZADO", 10)
        check(True, "al volver, el operario recibe el resultado de su servicio")
    finally:
        e.stop_all()


def t_central_crash():
    """Cae Central: las WS terminan el riego y se paran; al volver se recupera todo."""
    e, c, mons, engs, fo = boot("centralcrash", 21700, ("WS-01", "WS-02"), ("OP-01",))
    try:
        fo["OP-01"].send("2 WS-01 14")
        fo["OP-01"].wait_for("RIEGO AUTORIZADO", 10)
        time.sleep(3)
        c.kill()
        engs["WS-01"].wait_for("Imposible conectar con WM_Central", 10)
        check(True, "la WS muestra 'Imposible conectar con WM_Central'")
        mons["WS-01"].wait_for("Imposible conectar con WM_Central", 10)
        check(True, "el monitor muestra 'Imposible conectar con WM_Central'")
        engs["WS-01"].wait_for("RIEGO FINALIZADO", 20)
        check(True, "la WS sigue regando hasta finalizar con Central caída")
        s = engs["WS-01"].mark()
        engs["WS-01"].send("r 5")
        engs["WS-01"].wait_for("No se puede regar: Imposible conectar con WM_Central", 5, s)
        check(True, "no admite nuevas peticiones mientras Central está caída")
        time.sleep(2)
        s = fo["OP-01"].mark()
        fo["OP-01"].send("2 WS-02 5")
        fo["OP-01"].wait_for("Imposible conectar con WM_Central", 10, s)
        check(True, "el FO muestra 'Imposible conectar con WM_Central'")
        # reinicio de Central (misma BD)
        c2 = e.central(name="central2")
        c2.wait_for("WS registradas", 10)
        check(c2.has("Disconnected"), "al reiniciar, Central muestra las WS de su BD como Desconectadas")
        e.wait_state("WS-01", "AVAILABLE", 25); e.wait_state("WS-02", "AVAILABLE", 25)
        check(all(p.alive() for p in (mons["WS-01"], engs["WS-01"], fo["OP-01"])),
              "se recupera SIN reiniciar monitores, engines ni FO")
        time.sleep(2)
        fo["OP-01"].wait_for("RIEGO FINALIZADO en WS-01", 10)
        check(True, "Central procesa el resumen final que quedó en Kafka (no se pierde ninguna transacción)")
        rows = db_rows(e, "select status, volume from sessions where ws_id='WS-01' and status!='DENIED'")
        check(rows and rows[0]["status"] == "ENDED" and rows[0]["volume"] > 1, f"BD consistente: riego ENDED con {rows[0]['volume']:.2f} L")
        fo["OP-01"].send("2 WS-02 5")
        fo["OP-01"].wait_for("RIEGO AUTORIZADO", 10, len(fo["OP-01"].lines) - 1)
        check(True, "nuevas peticiones funcionan tras recuperar Central")
    finally:
        e.stop_all()


def t_file():
    """FO lee servicios de un fichero: espera 4 s tras cada conclusión."""
    e = Env("file", 21800)
    try:
        e.broker(); time.sleep(0.5)
        c = e.central(); c.wait_for("Conectado a Kafka", 15)
        m, p = e.monitor("WS-01"); e.engine("WS-01", p)
        e.wait_state("WS-01", "AVAILABLE", 20)
        path = os.path.join(e.logdir, "servicios.txt")
        open(path, "w").write("# prueba\nWS-01#4\nWS-02\nWS-01#4\n")
        fo = e.fo("OP-01", extra=["--file", path])
        fo.wait_for("Fichero completado", 60)
        txt = fo.text()
        check("Servicio 1/3" in txt and "Servicio 3/3" in txt, "se solicitan los 3 servicios en orden")
        check("2 riegos realizados, 1 denegados" in txt, "resumen: 2 riegos OK y 1 denegado (WS-02 desconectada)")
        ts = lambda pat: [time.mktime(time.strptime(re.match(r"(\d\d:\d\d:\d\d)", l).group(1), "%H:%M:%S"))
                          for l in fo.lines if re.search(pat, l)]
        fin = ts(r"RIEGO FINALIZADO|PETICIÓN DENEGADA"); srv = ts(r"--- Servicio [23]/3")
        gaps = [s - f for f, s in zip(fin, srv)]
        check(all(g >= 3 for g in gaps), f"espera ~4 s entre servicios (huecos: {gaps})")
    finally:
        e.stop_all()


def t_manual():
    """Riego manual desde el menú de la propia WS."""
    e, c, mons, engs, fo = boot("manual", 21900, ("WS-01",), ())
    try:
        engs["WS-01"].send("r 5")
        engs["WS-01"].wait_for("Petición de riego manual", 5)
        engs["WS-01"].wait_for("Riego AUTORIZADO por Central", 10)
        e.wait_state("WS-01", "WATERING", 8)
        check(True, "riego manual desde la WS autorizado por Central")
        engs["WS-01"].send("f")
        engs["WS-01"].wait_for("finalizado desde la WS", 5)
        e.wait_state("WS-01", "AVAILABLE", 8)
        check(True, "finalizar el riego desde el menú de la WS")
        time.sleep(1.5)
        rows = db_rows(e, "select source, reason from sessions")
        check(rows and rows[0]["source"] == "WS" and rows[0]["reason"] == "MANUAL_WS", "registrado en BD con origen WS")
    finally:
        e.stop_all()


def t_protocol():
    """Tramas <STX><DATA><ETX><LRC>: LRC erróneo -> NACK -> retransmisión."""
    import socket, threading
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "shared"))
    import wm_common as w
    a, b = socket.socketpair()
    A, B = w.FramedConn(a), w.FramedConn(b)
    check(w.lrc(b"ABC") == 0x41 ^ 0x42 ^ 0x43, "LRC = XOR byte a byte")
    got = []
    t = threading.Thread(target=lambda: got.append(B.recv(timeout=3)))
    t.start()
    bad = bytearray(w.build_frame("HB#WS-01#OK")); bad[-1] ^= 0xFF
    a.sendall(bytes(bad))
    check(a.recv(1) == w.NACK, "trama con LRC incorrecto -> el receptor responde NACK")
    a.sendall(w.build_frame("HB#WS-01#OK"))
    check(a.recv(1) == w.ACK, "trama correcta -> ACK")
    t.join()
    check(got == ["HB#WS-01#OK"], "mensaje recibido íntegro tras la retransmisión")
    t2 = threading.Thread(target=lambda: got.append(B.recv(timeout=3)))
    t2.start(); A.send("REGISTRO#WS-04#River Park"); t2.join()
    check(got[-1] == "REGISTRO#WS-04#River Park", "send()/recv() con ACK extremo a extremo")


TESTS = {"protocol": t_protocol, "basic": t_basic, "leak": t_leak, "block": t_block,
         "moncrash": t_monitor_crash, "monrestart": t_monitor_restart, "engine": t_engine_crash, "fo": t_fo_crash,
         "central": t_central_crash, "file": t_file, "manual": t_manual}


def main():
    names = sys.argv[1:] or list(TESTS)
    for n in names:
        print(f"\n=== {n}: {TESTS[n].__doc__.strip()}")
        try:
            TESTS[n]()
            RESULTS.append((n, True, ""))
        except Exception as ex:
            traceback.print_exc()
            RESULTS.append((n, False, str(ex)[:300]))
    print("\n================ RESUMEN ================")
    for n, ok, msg in RESULTS:
        print(("PASS " if ok else "FAIL ") + n + ("" if ok else "  -> " + msg.splitlines()[0]))
    sys.exit(0 if all(r[1] for r in RESULTS) else 1)


if __name__ == "__main__":
    main()
