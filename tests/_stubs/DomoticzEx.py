"""Minimal stand-in for the Domoticz-provided ``DomoticzEx`` module.

The real module only exists inside the Domoticz Python plugin runtime, so
the test suite injects this stub (its directory goes on ``sys.path`` ahead
of everything else) which lets ``plugin.py`` import and ``BasePlugin()``
construct without a running Domoticz.

It is deliberately permissive: logging calls are no-ops, the device/param
containers are plain dicts, and ``WebSocketSend`` is a spy that records
every payload so the WebSocket-migration tests (F1+) can assert the
protocol without a socket.
"""

import json as _json

# ── Logging (no-ops; flip DEBUG to echo while writing tests) ──────────────
DEBUG = False


def _log(prefix, *a):
    if DEBUG:
        print(prefix, *a)


def Debug(*a):  _log("DEBUG:", *a)
def Log(*a):    _log("LOG:", *a)
def Error(*a):  _log("ERROR:", *a)
def Status(*a): _log("STATUS:", *a)


def Heartbeat(_secs):  # accepted, ignored
    pass


def Trace(_on=True):
    pass


# ── Runtime-injected containers ──────────────────────────────────────────
Devices = {}
Parameters = {}
Settings = {}
Images = {}

# ── WebSocketSend spy (used by F1+ protocol tests) ───────────────────────
ws_sent = []  # list of payloads passed to WebSocketSend


def WebSocketSend(payload):
    # The plugin serializes frames with json.dumps and sends a string (it
    # avoids Domoticz's lossy dict->JSON path). Record the decoded object so
    # tests can introspect by key, mirroring what the browser does on receipt.
    if isinstance(payload, str):
        try:
            payload = _json.loads(payload)
        except Exception:
            pass
    ws_sent.append(payload)


def reset_ws():
    ws_sent.clear()


# ── Stub framework classes (only the surface plugin.py may touch) ────────
class Device:
    def __init__(self, *a, **k):
        self.Units = {}
        self.Name = k.get("Name", "")

    def Create(self):  pass
    def Update(self, *a, **k):  pass
    def Delete(self):  pass


class Unit:
    def __init__(self, *a, **k):
        self.__dict__.update(k)
        self.nValue = 0
        self.sValue = ""

    def Create(self):  pass
    def Update(self, *a, **k):  pass
    def Delete(self):  pass


class Connection:
    def __init__(self, *a, **k):
        self.__dict__.update(k)

    def Connect(self, *a, **k):  pass
    def Send(self, *a, **k):  pass
    def Disconnect(self, *a, **k):  pass


class Image:
    def __init__(self, *a, **k):
        self.__dict__.update(k)

    def Create(self):  pass
