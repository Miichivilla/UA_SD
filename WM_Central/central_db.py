"""Capa de persistencia SQLite de WM_Central."""
import os
import sqlite3
import threading
import time


class Db:
    def __init__(self, path: str):
        d = os.path.dirname(os.path.abspath(path))
        os.makedirs(d, exist_ok=True)
        self.path = path
        self.lock = threading.RLock()
        self.con = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.con.row_factory = sqlite3.Row
        with self.lock:
            self.con.execute("PRAGMA journal_mode=WAL")
            self.con.execute("PRAGMA synchronous=FULL")  # no perder transacciones
            self.con.executescript("""
            CREATE TABLE IF NOT EXISTS operators(
                id TEXT PRIMARY KEY, name TEXT NOT NULL DEFAULT '');
            CREATE TABLE IF NOT EXISTS watering_stations(
                id TEXT PRIMARY KEY, location TEXT NOT NULL,
                blocked INTEGER NOT NULL DEFAULT 0,
                total_liters REAL NOT NULL DEFAULT 0,
                registered_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS sessions(
                req_id TEXT PRIMARY KEY, ws_id TEXT NOT NULL, operator_id TEXT NOT NULL,
                source TEXT NOT NULL, duration INTEGER NOT NULL,
                started_at REAL, ended_at REAL, volume REAL NOT NULL DEFAULT 0,
                reason TEXT, status TEXT NOT NULL, created_at REAL NOT NULL);
            """)

    # -- operarios ----------------------------------------------------------
    def operators(self):
        with self.lock:
            return [dict(r) for r in self.con.execute("SELECT * FROM operators ORDER BY id")]

    def operator_exists(self, op_id):
        with self.lock:
            return self.con.execute("SELECT 1 FROM operators WHERE id=?", (op_id,)).fetchone() is not None

    def add_operator(self, op_id, name=""):
        with self.lock:
            self.con.execute("INSERT OR REPLACE INTO operators(id,name) VALUES(?,?)", (op_id, name))

    # -- estaciones ---------------------------------------------------------
    def stations(self):
        with self.lock:
            return [dict(r) for r in self.con.execute("SELECT * FROM watering_stations ORDER BY id")]

    def station(self, ws_id):
        with self.lock:
            r = self.con.execute("SELECT * FROM watering_stations WHERE id=?", (ws_id,)).fetchone()
            return dict(r) if r else None

    def add_station(self, ws_id, location):
        with self.lock:
            self.con.execute(
                "INSERT OR IGNORE INTO watering_stations(id,location,registered_at) VALUES(?,?,?)",
                (ws_id, location, time.time()))

    def set_blocked(self, ws_id, blocked):
        with self.lock:
            self.con.execute("UPDATE watering_stations SET blocked=? WHERE id=?", (1 if blocked else 0, ws_id))

    def add_liters(self, ws_id, liters):
        with self.lock:
            self.con.execute("UPDATE watering_stations SET total_liters=total_liters+? WHERE id=?",
                             (liters, ws_id))

    # -- sesiones de riego (historial de transacciones) ----------------------
    def session(self, req_id):
        with self.lock:
            r = self.con.execute("SELECT * FROM sessions WHERE req_id=?", (req_id,)).fetchone()
            return dict(r) if r else None

    def insert_session(self, req_id, ws_id, op_id, source, duration, status,
                       started_at=None, reason=None):
        with self.lock:
            self.con.execute(
                "INSERT OR IGNORE INTO sessions(req_id,ws_id,operator_id,source,duration,"
                "started_at,status,reason,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (req_id, ws_id, op_id, source, duration, started_at, status, reason, time.time()))

    def activate_session(self, req_id, started_at):
        with self.lock:
            self.con.execute("UPDATE sessions SET status='ACTIVE', started_at=? WHERE req_id=?",
                             (started_at, req_id))

    def update_volume(self, req_id, volume):
        with self.lock:
            self.con.execute("UPDATE sessions SET volume=? WHERE req_id=?", (volume, req_id))

    def end_session(self, req_id, volume, reason, status="ENDED"):
        with self.lock:
            self.con.execute(
                "UPDATE sessions SET volume=?, reason=?, status=?, ended_at=? WHERE req_id=?",
                (volume, reason, status, time.time(), req_id))

    def set_session_volume_final(self, req_id, volume, reason=None):
        with self.lock:
            if reason:
                self.con.execute("UPDATE sessions SET volume=?, reason=? WHERE req_id=?",
                                 (volume, reason, req_id))
            else:
                self.con.execute("UPDATE sessions SET volume=? WHERE req_id=?", (volume, req_id))

    def active_sessions(self):
        with self.lock:
            return [dict(r) for r in self.con.execute("SELECT * FROM sessions WHERE status='ACTIVE'")]

    def recent_sessions(self, n=15):
        with self.lock:
            return [dict(r) for r in self.con.execute(
                "SELECT * FROM sessions ORDER BY created_at DESC LIMIT ?", (n,))]
