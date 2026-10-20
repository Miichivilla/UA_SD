"""Utilidades para lanzar los módulos como procesos reales y observar su salida."""
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


class Proc:
    def __init__(self, name, argv, cwd, logdir, extra_env=None):
        self.name = name
        env = dict(os.environ)
        env["PYTHONPATH"] = os.path.join(ROOT, "tests", "fakelib")
        env["NO_COLOR"] = "1"
        env["PYTHONUNBUFFERED"] = "1"
        env.update(extra_env or {})
        self.lines = []
        self.p = subprocess.Popen([sys.executable, "-u", *argv], cwd=cwd, env=env, stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        self.logf = open(os.path.join(logdir, name + ".log"), "a")
        threading.Thread(target=self._reader, daemon=True).start()

    def _reader(self):
        for line in self.p.stdout:
            line = ANSI.sub("", line.rstrip("\n"))
            self.lines.append(line)
            self.logf.write(line + "\n")
            self.logf.flush()

    def send(self, text):
        self.p.stdin.write(text + "\n")
        self.p.stdin.flush()

    def text(self):
        return "\n".join(self.lines)

    def has(self, pattern, since=0):
        rx = re.compile(pattern)
        return any(rx.search(l) for l in self.lines[since:])

    def wait_for(self, pattern, timeout=15, since=0):
        rx = re.compile(pattern)
        end = time.time() + timeout
        while time.time() < end:
            for l in self.lines[since:]:
                if rx.search(l):
                    return l
            time.sleep(0.1)
        raise AssertionError(f"[{self.name}] no apareció /{pattern}/ en {timeout}s.\nÚltimas líneas:\n"
                             + "\n".join(self.lines[-15:]))

    def mark(self):
        return len(self.lines)

    def kill(self, sig=signal.SIGKILL):
        try:
            self.p.send_signal(sig)
            self.p.wait(5)
        except Exception:
            pass

    def alive(self):
        return self.p.poll() is None


class Env:
    """Un escenario completo: broker falso + central + N monitores/engines/FO."""

    def __init__(self, tag, base_port):
        self.tag = tag
        self.logdir = os.path.join(os.environ.get("WM_TEST_LOGS", "/tmp/wm_tests"), tag)
        shutil.rmtree(self.logdir, ignore_errors=True)
        os.makedirs(self.logdir)
        self.k_port, self.c_port, self.h_port = base_port, base_port + 1, base_port + 2
        self.next_engine_port = base_port + 10
        self.procs = []
        self.kafka = f"127.0.0.1:{self.k_port}"
        self.dbfile = os.path.join(self.logdir, "central.db")

    def _spawn(self, name, argv, cwd, extra_env=None):
        p = Proc(name, argv, os.path.join(ROOT, cwd), self.logdir, extra_env)
        self.procs.append(p)
        return p

    def broker(self):
        return self._spawn("broker", [os.path.join(ROOT, "tests", "fake_broker.py"), str(self.k_port)], ".")

    def central(self, name="central", extra_env=None):
        return self._spawn(name, ["WM_Central.py", "--port", str(self.c_port), "--kafka", self.kafka,
                                  "--http-port", str(self.h_port), "--db", self.dbfile], "WM_Central", extra_env)

    def monitor(self, ws_id, port=None, register=None, name=None):
        port = port or self.alloc_port()
        argv = ["WM_WS_M.py", "--id", ws_id, "--central", f"127.0.0.1:{self.c_port}", "--engine-port", str(port)]
        if register:
            argv += ["--register", "--location", register]
        return self._spawn(name or f"{ws_id}_M", argv, "WM_WS", {"WM_UI": "0"}), port

    def engine(self, ws_tag, mon_port, name=None):
        data = os.path.join(self.logdir, "data_" + ws_tag)
        return self._spawn(name or f"{ws_tag}_E", ["WM_WS_E.py", "--kafka", self.kafka,
                                                   "--monitor", f"127.0.0.1:{mon_port}", "--data-dir", data], "WM_WS")

    def fo(self, op, name=None, extra=None):
        return self._spawn(name or f"FO_{op}", ["WM_FO.py", "--kafka", self.kafka, "--id", op, *(extra or [])],
                           "WM_FO")

    def alloc_port(self):
        self.next_engine_port += 1
        return self.next_engine_port

    # -- API del panel de Central
    def state(self):
        with urllib.request.urlopen(f"http://127.0.0.1:{self.h_port}/api/state", timeout=5) as r:
            return json.load(r)

    def ws_state(self, ws_id):
        for w in self.state()["wss"]:
            if w["id"] == ws_id:
                return w
        return None

    def wait_state(self, ws_id, state, timeout=15):
        end = time.time() + timeout
        last = None
        while time.time() < end:
            try:
                last = self.ws_state(ws_id)
                if last and last["state"] == state:
                    return last
            except Exception:
                pass
            time.sleep(0.25)
        raise AssertionError(f"{ws_id} no llegó a {state} (último: {last})")

    def api(self, path, body):
        req = urllib.request.Request(f"http://127.0.0.1:{self.h_port}{path}", data=json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=5) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            return json.load(e)

    def stop_all(self):
        for p in self.procs:
            p.kill()
