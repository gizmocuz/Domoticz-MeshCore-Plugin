"""
<plugin key="MeshCore" name="MeshCore" author="galadril" version="0.0.1" wikilink="" externallink="https://github.com/galadril/Domoticz-MeshCore-Plugin">
    <description>
        MeshCore LoRa mesh integration for Domoticz.
        Requires: pip install -r requirements.txt
    </description>
    <params>
        <param field="Mode1" label="Transport" width="120px">
            <options>
                <option label="TCP"    value="TCP" default="true"/>
                <option label="Serial" value="Serial"/>
            </options>
        </param>
        <param field="Address"    label="MeshCore Host" width="200px" default="192.168.1.50" visible_when="Mode1=TCP"/>
        <param field="Port"       label="MeshCore Port" width="80px"  default="5000"         visible_when="Mode1=TCP"/>
        <param field="SerialPort" label="Serial Port"   width="200px"                        visible_when="Mode1=Serial"/>
        <param field="Mode2"      label="Baud Rate"     width="100px"                        visible_when="Mode1=Serial">
            <options>
                <option label="115200" value="115200" default="true"/>
                <option label="57600"  value="57600"/>
                <option label="38400"  value="38400"/>
                <option label="19200"  value="19200"/>
                <option label="9600"   value="9600"/>
            </options>
        </param>
        <param field="Mode4"    label="Install Custom Dashboard" width="150px">
            <options>
                <option label="Yes" value="true" default="true"/>
                <option label="No"  value="false"/>
            </options>
        </param>
        <param field="Mode6"   label="Debug Level" width="150px">
            <options>
                <option label="None"  value="0" default="true"/>
                <option label="Basic" value="62"/>
                <option label="All"   value="-1"/>
            </options>
        </param>
    </params>
</plugin>
"""

import Domoticz
import asyncio
import collections
import gc
import json
import os
import queue
import threading
import time
import traceback

try:
    from meshcore import MeshCore
    from meshcore.events import EventType
    MESHCORE_AVAILABLE = True
except ImportError:
    MESHCORE_AVAILABLE = False

# ── Device unit scheme ────────────────────────────────────────────────────────
# Units 1-9: global devices
# Units 10+: NODE_SLOTS slots per node (index 0 = self node, 1..N = tracked nodes)
UNIT_INBOX      = 1
UNIT_SEND       = 2   # Text device: write "[node: ]message" here to send
UNIT_MSGS_RECV  = 3   # Custom counter: messages received today
UNIT_MSGS_SENT_ = 4   # Custom counter: messages sent today

NODE_BASE  = 10
NODE_SLOTS = 20   # device slots reserved per node (max 11 nodes → unit 219)

# MeshCore firmware exposes up to 40 channel slots. Domoticz devices are NOT
# created per slot — they live entirely in the dashboard JSON map.
MAX_CHANNEL_SLOTS = 40

# Offsets within each node's slot block
OFF_STATUS    = 0   # Switch:      online / offline
OFF_BATT_PCT  = 1   # Percentage:  battery %
OFF_BATT_V    = 2   # Custom (V):  battery voltage
OFF_RSSI      = 3   # Custom (dBm): last RSSI
OFF_SNR       = 4   # Custom (dB):  last SNR
OFF_NOISE     = 5   # Custom (dBm): noise floor
OFF_LASTSEEN  = 6   # Text:        timestamp of last received message/advert
OFF_TEMP      = 7   # Temperature: °C
OFF_HUMID     = 8   # Humidity:    %
OFF_HOPS      = 9   # Custom:      path length (hops)
OFF_UPTIME    = 10  # Custom (min): node uptime
OFF_AIRTIME   = 11  # Custom (%):  TX airtime utilization
OFF_MSGS_SENT = 12  # Custom:      total messages sent
OFF_MSGS_RECV = 13  # Custom:      total messages received

# Cayenne LPP sensor type codes (used in self_telemetry LPP list entries)
LPP_TEMPERATURE = 103
LPP_HUMIDITY    = 104
LPP_VOLTAGE     = 116   # channel 1 = battery

# Battery voltage range for % calculation (mV)
BAT_VMIN_MV = 3000
BAT_VMAX_MV = 4200

# Node is considered online if last_advert is newer than this (8 h)
ONLINE_THRESHOLD_S = 28800

# Connection timeout for the initial connect (seconds)
CONNECT_TIMEOUT    = 12
COMMAND_TIMEOUT    = 10

# Reconnect delay after a connection failure / drop (seconds)
RECONNECT_DELAY_S  = 30

# Periodic refresh intervals on the persistent connection
STATS_REFRESH_S    = 300   # self-node stats (battery, radio, packets)
CONTACTS_REFRESH_S = 60    # contact list refresh (catches new contacts + path changes)

# Rolling RX log buffer size (per-event detail kept in memory for the dashboard)
RX_LOG_BUFFER      = 250
# How often we re-write meshcore_rx_log.json at most (seconds)
RX_LOG_WRITE_S     = 2.0

# After the user changes a setting, ignore device-side self_info echoes of
# manual_add_contacts/telemetry/adv_loc_policy for this many seconds. Some
# firmware briefly returns the prior value while flushing to flash, which
# would otherwise undo the user's change on the very next poll.
# Note: this only guards self_info-sourced settings. The default flood scope
# comes from a separate get_default_flood_scope() round-trip and the device
# returns the just-written value reliably there, so no grace needed for it.
SETTINGS_GRACE_S = 45


def _bat_pct(mv: int) -> int:
    return max(0, min(100, int((mv - BAT_VMIN_MV) / (BAT_VMAX_MV - BAT_VMIN_MV) * 100)))


class BasePlugin:

    def __init__(self):
        self._queue          = queue.Queue()  # worker → main thread
        self.transport       = "TCP"          # "TCP" or "Serial"
        self.host            = ""
        self.port            = 5000
        self.serial_port     = ""
        self.baud_rate       = 115200
        # Tracks last successful-connection state to emit transition log lines
        self._was_connected  = False
        self._contact_names  = []   # contact names discovered from mc.contacts (non-self)
        self.initialized     = False
        self._self_name      = ""   # name of the connected node
        # pubkey_prefix (12 hex chars) → adv_name, rebuilt from contacts
        self._prefix_to_name = {}
        # node_name → last Unix timestamp we saw ANY activity from it
        self._node_last_activity: dict = {}
        # node_name → {"lat": float, "lon": float} from contact adv_lat/adv_lon
        self._node_locations: dict = {}
        # node_name → contact type int (1=Client/Contact, 2=Repeater, 3=Room Server, 4=Sensor)
        self._node_types: dict = {}
        # node_name → last_advert unix timestamp (for client-side sorting)
        self._node_last_advert: dict = {}
        # node_name → contact public_key hex (needed for remove_contact)
        self._node_pubkey: dict = {}
        # Current value of the connected node's manual_add_contacts setting
        # (True = node ignores adverts from unknown contacts; False = auto-add)
        self._manual_add_contacts: bool = False
        # Other device settings mirrored from self_info — used by the dashboard
        self._telemetry_mode_base: int = 0
        self._telemetry_mode_loc:  int = 0
        self._telemetry_mode_env:  int = 0
        self._advert_loc_policy:   int = 0
        # Monotonic time (seconds) of the last user-driven setting change.
        # Within SETTINGS_GRACE_S after a change we trust the just-set value
        # over what self_info reports — some firmware returns the previous
        # value briefly while flushing to flash.
        self._settings_set_at: float = 0.0
        # Default flood scope tag (e.g. "#nl"); empty string = global flood.
        # Read via mc.commands.get_default_flood_scope() once per poll cycle.
        self._default_flood_scope: str = ""
        # Favorite contact names — persisted to <plugin_dir>/meshcore_favorites.json.
        # Toggled from the dashboard; favorites sort first within online/offline groups.
        self._favorites: set = set()
        # Device info (firmware version, build, model) from send_device_query().
        # Fetched on connect and refreshed periodically.
        self._device_info: dict = {}
        # Full SELF_INFO snapshot — radio params (freq, bw, sf, cr, tx_power,
        # max_tx_power, multi_acks), our pubkey + advertised lat/lon. Exposed
        # via the device map so the dashboard's self-node side panel can show
        # and edit them.
        self._self_info_full: dict = {}
        # Latest result of get_self_telemetry (board-level sensors if any).
        self._self_telemetry: dict = {}
        # Per-contact query results from remote sync calls (status, telemetry,
        # neighbours). Keyed by contact adv_name. Exposed via device map so
        # the dashboard's contact info panel can read them.
        self._contact_query_results: dict = {}
        # Bumped from fast (2s) to steady (10s) once the first contacts batch
        # has been dispatched, so we keep onHeartbeat responsive without
        # firing every 2 seconds forever.
        self._heartbeat_restored: bool = False
        # Last sValue we already dispatched — prevents re-sending on every heartbeat
        self._last_sent_text = ""
        # Message counters (reset when Domoticz restarts the plugin)
        self._recv_count = 0
        self._sent_count = 0
        # Channel names already fetched flag (only need once)
        self._channels_fetched = False
        # Channel index→name map (populated from device), e.g. {0: "General", 1: "MyRoom"}
        # Non-empty entries only — used for message routing.
        self._channel_names: dict = {}
        # Full 8-slot table, including empty slots (idx → name). Exposed via
        # the device map so the dashboard can render every slot with controls.
        self._channel_slots: dict = {}
        # Persistent-connection worker state
        self._worker_thread: threading.Thread | None = None
        self._worker_loop: asyncio.AbstractEventLoop | None = None
        self._mc = None                       # live MeshCore instance (worker-owned)
        self._stop_event = threading.Event()  # set on shutdown (cross-thread)
        self._stop_async: asyncio.Event | None = None  # created inside worker loop
        # Serialise concurrent `!verb` sends and remote queries. The meshcore
        # library subscribes to EventType.OK/ERROR globally per send() call,
        # so two commands in flight at the same time can have their responses
        # cross-attributed (the second waiter gets the first reply). One lock
        # → one in-flight command keeps the dispatcher unambiguous.
        self._cmd_lock: asyncio.Lock | None = None
        # Flag to prevent new connections during shutdown
        self._stopping = False
        # All fields below are touched from BOTH the worker thread (push
        # event callbacks) AND the main thread (_handle_message via
        # _dispatch, plus _write_rx_log). Always take self._rx_log_lock
        # before reading or mutating any of them.
        # Rolling RX_LOG_DATA buffer.
        self._rx_log = collections.deque(maxlen=RX_LOG_BUFFER)
        self._rx_log_lock = threading.Lock()
        self._rx_log_dirty = False
        self._rx_log_last_write = 0.0
        # Aggregated stats over the rx-log window:
        self._payload_type_counts: dict = collections.defaultdict(int)
        self._chan_hash_counts:    dict = collections.defaultdict(int)
        # pubkey_prefix → list of {t, snr, rssi, path_len, kind}
        self._signal_history:      dict = collections.defaultdict(list)
        # raw_hex → list of {t, path, snr} (duplicate flood detection)
        self._dup_floods:          dict = collections.defaultdict(list)
        # 24h × 1h heatmap: hour-of-day → count for past 24h (timestamps trimmed on read)
        self._packet_times = collections.deque(maxlen=2000)  # raw ts list, trimmed to last 24h

    def _force_close_serial(self, mc):
        """Synchronously close the underlying pyserial port.

        Used only on shutdown so Windows releases the COM handle immediately
        instead of waiting for connection_lost to fire on a loop we're about
        to tear down.
        """
        try:
            connection = mc.connection_manager.connection
            transport = getattr(connection, "transport", None)
            if transport is None:
                return
            raw_serial = getattr(transport, "serial", None)
            try:
                transport.close()
            except Exception:
                pass
            if raw_serial is not None:
                try:
                    raw_serial.close()
                except Exception:
                    pass
        except Exception:
            pass

    def _node_index(self, node_name: str) -> int:
        """Return slot index: 0 = self node, 1..N = contacts."""
        if node_name == self._self_name:
            return 0
        if node_name in self._contact_names:
            return self._contact_names.index(node_name) + 1
        return -1

    def _node_unit(self, node_idx: int, offset: int) -> int:
        return NODE_BASE + node_idx * NODE_SLOTS + offset

    def _all_node_names(self):
        """Self node (if known) + all discovered contacts."""
        names = []
        if self._self_name:
            names.append(self._self_name)
        names.extend(self._contact_names)
        return names

    async def _disconnect_mc(self, mc):
        """Graceful disconnect of the persistent connection."""
        if mc is None:
            return
        try:
            await asyncio.wait_for(mc.disconnect(), timeout=5)
        except Exception as exc:
            Domoticz.Debug(f"disconnect error: {exc}")
        if self.transport == "Serial":
            # Force-close the raw pyserial Serial so Windows releases the COM
            # handle right away instead of waiting for connection_lost.
            self._force_close_serial(mc)
            try:
                await asyncio.sleep(0.1)
            except Exception:
                pass

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def onStart(self):
        if not MESHCORE_AVAILABLE:
            Domoticz.Error("meshcore package not installed. Run: pip install meshcore")
            return

        Domoticz.Debugging(int(Parameters["Mode6"] or 0))
        self.transport = (Parameters.get("Mode1") or "TCP").strip() or "TCP"
        self.host      = Parameters.get("Address", "").strip()
        self.port      = int((Parameters.get("Port") or "5000").strip() or 5000)
        self.serial_port = Parameters.get("SerialPort", "").strip()
        try:
            self.baud_rate = int((Parameters.get("Mode2") or "115200").strip() or 115200)
        except ValueError:
            self.baud_rate = 115200

        if self.transport == "Serial" and not self.serial_port:
            Domoticz.Error("Serial transport selected but no serial port chosen.")
            return

        self._create_base_devices()
        self._load_manual_locations()
        self._load_favorites()

        if Parameters.get("Mode4", "true") == "true":
            self._install_custom_page()
            self._install_manual_locations()

        self.initialized = True
        # Heartbeat is now purely for draining the worker→main queue; the
        # actual MeshCore session is a persistent connection in a worker
        # thread. Use a fast tick at startup so the first contacts/self_info
        # batch is dispatched promptly, then `_dispatch` bumps it back to a
        # steady-state cadence once the first contacts arrive.
        Domoticz.Heartbeat(2)
        self._heartbeat_restored = False
        if self.transport == "Serial":
            Domoticz.Log(f"MeshCore plugin started - Serial {self.serial_port} @ {self.baud_rate}")
        else:
            Domoticz.Log(f"MeshCore plugin started - TCP {self.host}:{self.port}")

        self._stop_event.clear()
        t = threading.Thread(target=self._worker_main, daemon=True, name="MeshCoreWorker")
        self._worker_thread = t
        t.start()

    def onStop(self):
        self._stopping = True
        self.initialized = False
        self._stop_event.set()

        loop = self._worker_loop
        stop_async = self._stop_async
        if loop is not None and loop.is_running() and stop_async is not None:
            # Wake the worker loop via its asyncio.Event so the serve loop
            # exits at its next iteration, runs its finally block (which
            # disconnects mc and drains serial_asyncio_fast's executor tasks)
            # and lets the thread shut down cleanly.
            try:
                loop.call_soon_threadsafe(stop_async.set)
            except Exception as exc:
                Domoticz.Debug(f"onStop: stop dispatch failed: {exc}")

        # Wait for the worker thread to exit
        t = self._worker_thread
        if t is not None and t.is_alive():
            t.join(timeout=8)
            if t.is_alive():
                Domoticz.Log("onStop: worker thread did not stop within 8s.")

        # Force-close the serial port so Windows releases the COM handle even
        # if the cancelled tasks never reached the graceful disconnect path.
        if self.transport == "Serial" and self._mc is not None:
            self._force_close_serial(self._mc)
        self._mc = None
        self._worker_thread = None
        self._stop_async = None
        # Drop references that could keep an IOCP / proactor pinned alive and
        # force a collection so the Windows COM port handle is released by
        # the asyncio internals.
        gc.collect()
        # Brief grace window — the foreign (Dummy-N) IOCP thread serving the
        # asyncio proactor on Windows can take a moment to die after the
        # loop's underlying handle is released. Give it 500 ms; not strictly
        # required but avoids Domoticz spamming "1 Python thread still
        # running" for ten seconds in the common case.
        for _ in range(10):
            alive_now = [t for t in threading.enumerate()
                         if t is not threading.main_thread()
                         and t.name != "MeshCoreWorker"]
            if not alive_now:
                break
            time.sleep(0.05)

        alive = [t for t in threading.enumerate() if t is not threading.main_thread()]
        Domoticz.Debug(f"onStop: {len(alive)} non-main thread(s) alive: " +
                       ", ".join(f"{t.name}(daemon={t.daemon})" for t in alive))

        self._remove_custom_page()
        Domoticz.Log("MeshCore plugin stopped.")

        # Workaround: Domoticz polls threading.enumerate() after onStop and
        # logs "Plugin has N Python threads still running" if anything besides
        # MainThread is reported. Our Domoticz plugin-host C thread (the one
        # currently running onStop) shows up as `Dummy-N` because it was
        # created in C and Python only registered it on first call-in. After
        # onStop returns this thread does exit in C, but Python's
        # threading._active dict never gets the entry pruned — so enumerate()
        # keeps reporting it for ~10s until Domoticz gives up. We pre-empt
        # that by removing the entry ourselves right before returning. It's
        # the very last thing we touch on this thread.
        try:
            my_tid = threading.get_ident()
            me = threading._active.get(my_tid)
            if me is None:
                Domoticz.Debug("DummyThread cleanup: no entry for current thread (already gone).")
            elif type(me).__name__ == "_DummyThread":
                del threading._active[my_tid]
                Domoticz.Debug("DummyThread cleanup: removed entry to silence Domoticz still-running warning.")
            else:
                # A future CPython rename of _DummyThread would land us here.
                # Log so the workaround silently stopping working is visible.
                Domoticz.Debug(
                    f"DummyThread cleanup: current thread is {type(me).__name__!r}, not _DummyThread — "
                    f"workaround did not run. Domoticz may report 'thread still running' until it gives up."
                )
        except Exception as exc:
            Domoticz.Debug(f"DummyThread cleanup: unexpected error: {exc}")

    def onHeartbeat(self):
        if not self.initialized or self._stopping:
            return

        # Drain results from the worker thread (device updates, logging)
        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break
            self._dispatch(item)

    def onDeviceModified(self, unit: int):
        if unit != UNIT_SEND or self._stopping:
            return
        dev = Devices.get(UNIT_SEND)
        if not dev or not dev.sValue:
            return
        text = dev.sValue.strip()
        if not text:
            return
        if text == self._last_sent_text:
            return
        self._last_sent_text = text

        # ── Short-circuit purely-local commands that don't need a radio session.
        # !favorite only touches plugin-side state + the favorites JSON file.
        if self._handle_local_only_command(text):
            return

        loop = self._worker_loop
        if loop is None:
            Domoticz.Error("Send failed: not connected to MeshCore device yet (will retry on reconnect).")
            self._queue.put(("send_result", {
                "ok": False, "target": "?", "body": text,
                "result": "not connected — auto-reconnect in progress"
            }))
            return

        # Internal "!"-commands have their own per-verb success log in the
        # worker; don't double-log them here. User-typed messages still get
        # a "Sending …" line so the inbox flow is traceable.
        if text.startswith("!"):
            Domoticz.Debug(f"Sending command: {text}")
        else:
            Domoticz.Log(f"Sending message: {text}")
        # Schedule the send on the worker's event loop. The coroutine re-reads
        # self._mc and checks is_connected itself — the worker may disconnect
        # between this scheduling call and the coroutine running, and we don't
        # want a TOCTOU here.
        try:
            asyncio.run_coroutine_threadsafe(self._send_message_for_text(text), loop)
        except Exception as exc:
            Domoticz.Error(f"Send scheduling failed: {exc}")
            self._queue.put(("send_result", {
                "ok": False, "target": "?", "body": text, "result": str(exc)
            }))

    def _handle_local_only_command(self, text: str) -> bool:
        """Handle commands that don't require an MC connection. Returns True if
        consumed (caller should not spawn a send worker).

        Note: we intentionally don't enqueue a "send_result" here — the
        dashboard already performs an optimistic update of its local
        _deviceMap.favorites array on click, so it doesn't need confirmation
        roundtripping through the queue.
        """
        if not text.startswith("!favorite "):
            return False
        try:
            _, action, name = text.split(None, 2)
        except ValueError:
            Domoticz.Error("!favorite syntax: !favorite add|remove <name>")
            return True
        action = action.lower()
        if action == "add":
            self._favorites.add(name)
        elif action == "remove":
            self._favorites.discard(name)
        else:
            Domoticz.Error(f"!favorite unknown action: {action}")
            return True
        self._save_favorites()
        self._write_device_map()
        Domoticz.Debug(f"Favorite {action}: {name}")
        return True

    # ── Device creation ───────────────────────────────────────────────────────

    def _create_base_devices(self):
        if UNIT_INBOX not in Devices:
            Domoticz.Device(Name="Mesh Inbox", Unit=UNIT_INBOX, TypeName="Text").Create()
        if UNIT_SEND not in Devices:
            Domoticz.Device(Name="Mesh Send",  Unit=UNIT_SEND,  TypeName="Text").Create()
        if UNIT_MSGS_RECV not in Devices:
            Domoticz.Device(Name="Mesh Msgs Received", Unit=UNIT_MSGS_RECV,
                            TypeName="Custom", Options={"Custom": "1;msgs"}).Create()
        if UNIT_MSGS_SENT_ not in Devices:
            Domoticz.Device(Name="Mesh Msgs Sent", Unit=UNIT_MSGS_SENT_,
                            TypeName="Custom", Options={"Custom": "1;msgs"}).Create()

    def _favorites_path(self) -> str:
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), "meshcore_favorites.json")

    def _load_favorites(self):
        p = self._favorites_path()
        if not os.path.isfile(p):
            return
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                self._favorites = {str(x) for x in data}
                Domoticz.Log(f"Loaded {len(self._favorites)} favorite contact(s).")
        except Exception as exc:
            Domoticz.Error(f"Could not load meshcore_favorites.json: {exc}")

    def _save_favorites(self):
        p = self._favorites_path()
        try:
            with open(p, "w", encoding="utf-8") as f:
                json.dump(sorted(self._favorites), f)
        except Exception as exc:
            Domoticz.Error(f"Could not write meshcore_favorites.json: {exc}")

    def _load_manual_locations(self):
        """Load meshcore_locations.json from the plugin directory as seed locations.

        Format: {"NodeName": {"lat": 52.123, "lon": 4.567}, ...}
        These are used as fallback — live GPS data from contacts overwrites them.
        """
        loc_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "meshcore_locations.json")
        if not os.path.isfile(loc_file):
            return
        try:
            with open(loc_file, "r") as f:
                manual = json.load(f)
            for name, loc in manual.items():
                lat = loc.get("lat", 0)
                lon = loc.get("lon", 0)
                if lat and lon:
                    self._node_locations.setdefault(name, {"lat": lat, "lon": lon})
            Domoticz.Log(f"Loaded manual locations for {len(manual)} node(s) from meshcore_locations.json")
        except Exception as exc:
            Domoticz.Error(f"Could not load meshcore_locations.json: {exc}")

    def _ensure_node_devices(self, node_name: str):
        """Create per-node devices on first data for that node."""
        idx = self._node_index(node_name)
        if idx < 0:
            return
        is_self = (idx == 0)
        if is_self:
            specs = [
                (OFF_STATUS,    f"{node_name} Status",      "Switch",      {}),
                (OFF_BATT_PCT,  f"{node_name} Battery",     "Percentage",  {}),
                (OFF_BATT_V,    f"{node_name} Battery V",   "Custom",      {"Custom": "1;V"}),
                (OFF_RSSI,      f"{node_name} RSSI",        "Custom",      {"Custom": "1;dBm"}),
                (OFF_SNR,       f"{node_name} SNR",         "Custom",      {"Custom": "1;dB"}),
                (OFF_NOISE,     f"{node_name} Noise Floor", "Custom",      {"Custom": "1;dBm"}),
                (OFF_LASTSEEN,  f"{node_name} Last Seen",   "Text",        {}),
                (OFF_UPTIME,    f"{node_name} Uptime",      "Custom",      {"Custom": "1;min"}),
                (OFF_AIRTIME,   f"{node_name} Airtime TX",  "Custom",      {"Custom": "1;s"}),
                (OFF_MSGS_SENT, f"{node_name} Pkts Sent",   "Custom",      {"Custom": "1;pkts"}),
                (OFF_MSGS_RECV, f"{node_name} Pkts Recv",   "Custom",      {"Custom": "1;pkts"}),
            ]
        else:
            # Remote contacts: only data reliably available from contacts list and messages
            specs = [
                (OFF_STATUS,   f"{node_name} Status",    "Switch", {}),
                (OFF_SNR,      f"{node_name} SNR",       "Custom", {"Custom": "1;dB"}),
                (OFF_LASTSEEN, f"{node_name} Last Seen", "Text",   {}),
                (OFF_HOPS,     f"{node_name} Hops",      "Custom", {"Custom": "1;hops"}),
            ]
        created = False
        skipped = False
        for offset, name, typename, opts in specs:
            unit = self._node_unit(idx, offset)
            # Domoticz only allows units 1..255 per plugin instance. With the
            # current 20-slot block size that caps us at ~11 remote contacts.
            # Beyond that we silently skip Device creation — the contact still
            # appears in the dashboard via the device map JSON.
            if unit < 1 or unit > 255:
                skipped = True
                continue
            if unit not in Devices:
                Domoticz.Device(Name=name, Unit=unit, TypeName=typename, Options=opts).Create()
                created = True
        if created:
            Domoticz.Log(f"Created devices for node '{node_name}' (idx={idx})")
            self._write_device_map()
        elif skipped and not getattr(self, "_warned_overflow", False):
            Domoticz.Log(
                f"Skipping Domoticz devices for node '{node_name}' (idx={idx}) — "
                "unit numbers would exceed 255. The contact still appears on the dashboard."
            )
            self._warned_overflow = True

    # ── Custom dashboard page ──────────────────────────────────────────────────

    def _install_custom_page(self):
        plugin_dir = os.path.dirname(os.path.abspath(__file__))
        template = os.path.join(plugin_dir, "meshcore.html")
        if not os.path.isfile(template):
            Domoticz.Error("meshcore.html template not found — dashboard not installed.")
            return

        domoticz_root = os.path.abspath(os.path.join(plugin_dir, "..", ".."))
        dest_dir      = os.path.join(domoticz_root, "www", "templates")
        dest          = os.path.join(dest_dir, "meshcore.html")

        try:
            import shutil
            os.makedirs(dest_dir, exist_ok=True)
            shutil.copy2(template, dest)
            Domoticz.Log(f"MeshCore dashboard installed: {dest}")
        except Exception as exc:
            Domoticz.Error(f"Failed to install dashboard: {exc}")

        # Bundle Leaflet locally so the topology / map panel works even when
        # the browser's tracking-prevention blocks unpkg.com (Edge/Firefox).
        leaflet_src = os.path.join(plugin_dir, "assets", "leaflet")
        leaflet_dst = os.path.join(dest_dir, "leaflet")
        if os.path.isdir(leaflet_src):
            try:
                import shutil
                os.makedirs(leaflet_dst, exist_ok=True)
                for fname in ("leaflet.js", "leaflet.css"):
                    s = os.path.join(leaflet_src, fname)
                    if os.path.isfile(s):
                        shutil.copy2(s, os.path.join(leaflet_dst, fname))
                Domoticz.Debug(f"Leaflet installed: {leaflet_dst}")
            except Exception as exc:
                Domoticz.Error(f"Failed to install Leaflet: {exc}")

    def _install_manual_locations(self):
        """Copy meshcore_locations.json to the templates dir so the dashboard can fetch it."""
        plugin_dir    = os.path.dirname(os.path.abspath(__file__))
        src = os.path.join(plugin_dir, "meshcore_locations.json")
        if not os.path.isfile(src):
            return
        domoticz_root = os.path.abspath(os.path.join(plugin_dir, "..", ".."))
        dest = os.path.join(domoticz_root, "www", "templates", "meshcore_locations.json")
        try:
            import shutil
            shutil.copy2(src, dest)
        except Exception as exc:
            Domoticz.Debug(f"Could not install meshcore_locations.json: {exc}")

    def _write_channel_names(self, channel_names: dict):
        """Write channel index→name map as JSON for the dashboard to fetch."""
        plugin_dir    = os.path.dirname(os.path.abspath(__file__))
        domoticz_root = os.path.abspath(os.path.join(plugin_dir, "..", ".."))
        dest = os.path.join(domoticz_root, "www", "templates", "meshcore_channels.json")
        try:
            with open(dest, "w") as f:
                json.dump(channel_names, f)
        except Exception as exc:
            Domoticz.Debug(f"Could not write channel names: {exc}")

    def _write_device_map(self):
        """Write meshcore_devices.json so the dashboard can look up devices by idx
        rather than by name — rename-proof and collision-free.

        Format:
        {
          "inbox": <idx>,
          "self": "<node_name>",          # or null
          "nodes": {
            "<node_name>": {
              "status":    <idx|null>,
              "battery":   <idx|null>,
              "battery_v": <idx|null>,
              "rssi":      <idx|null>,
              "snr":       <idx|null>,
              "noise":     <idx|null>,
              "last_seen": <idx|null>,
              "hops":      <idx|null>,
              "uptime":    <idx|null>,
              "airtime":   <idx|null>,
              "pkts_sent": <idx|null>,
              "pkts_recv": <idx|null>
            },
            ...
          }
        }
        """
        def _slot(unit):
            """Return {idx, value, online} for a device unit, or None if not created yet."""
            d = Devices.get(unit)
            if not d:
                return None
            return {
                "idx":    d.ID,
                "value":  d.sValue if d.sValue else None,
                "online": d.nValue == 1,
            }

        nodes = {}
        for node_name in self._all_node_names():
            ni = self._node_index(node_name)
            if ni < 0:
                continue
            loc = self._node_locations.get(node_name, {})
            pk_full = self._node_pubkey.get(node_name, "")
            # Build the last_seen slot. _node_last_activity is the source of
            # truth — it's always updated regardless of whether the Domoticz
            # device exists (contacts with unit > 255 don't get devices) or
            # whether the prefix lookup matched a previous code path. We
            # surface the matching Domoticz device idx for the click-through
            # to the device log when the unit was created.
            ls_slot_dev = _slot(self._node_unit(ni, OFF_LASTSEEN))
            last_activity = self._node_last_activity.get(node_name, 0)
            if last_activity:
                ls_slot = {
                    "idx": ls_slot_dev.get("idx") if ls_slot_dev else None,
                    "value": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last_activity)),
                    "online": False,
                }
            else:
                ls_slot = ls_slot_dev
            nodes[node_name] = {
                "status":    _slot(self._node_unit(ni, OFF_STATUS)),
                "battery":   _slot(self._node_unit(ni, OFF_BATT_PCT)),
                "battery_v": _slot(self._node_unit(ni, OFF_BATT_V)),
                "rssi":      _slot(self._node_unit(ni, OFF_RSSI)),
                "snr":       _slot(self._node_unit(ni, OFF_SNR)),
                "noise":     _slot(self._node_unit(ni, OFF_NOISE)),
                "last_seen": ls_slot,
                "hops":      _slot(self._node_unit(ni, OFF_HOPS)),
                "uptime":    _slot(self._node_unit(ni, OFF_UPTIME)),
                "airtime":   _slot(self._node_unit(ni, OFF_AIRTIME)),
                "pkts_sent": _slot(self._node_unit(ni, OFF_MSGS_SENT)),
                "pkts_recv": _slot(self._node_unit(ni, OFF_MSGS_RECV)),
                "lat":       loc.get("lat"),
                "lon":       loc.get("lon"),
                # Contact metadata — used by the dashboard for type chip and sorting.
                # type: 1=Contact, 2=Repeater, 3=Room Server, 4=Sensor (0 for self)
                "type":          self._node_types.get(node_name, 0),
                "last_advert":   self._node_last_advert.get(node_name, 0),
                # Full pubkey (hex) and first-12-char prefix used by the firmware
                # to identify message senders. The sparkline / signal-history
                # lookup is keyed by the prefix.
                "pubkey":        pk_full,
                "pubkey_prefix": pk_full[:12] if pk_full else "",
                # Per-contact query results (status / telemetry / neighbours)
                # from req_* sync calls. None if never queried.
                "query":         self._contact_query_results.get(node_name, {}),
            }

        inbox_dev = Devices.get(UNIT_INBOX)
        send_dev  = Devices.get(UNIT_SEND)
        payload = {
            "inbox":        inbox_dev.ID if inbox_dev else None,
            "inbox_value":  inbox_dev.sValue if inbox_dev else None,
            "send_idx":     send_dev.ID if send_dev else None,
            "self":         self._self_name or None,
            "nodes":        nodes,
            # True = device blocks auto-add of new contacts from adverts
            "manual_add_contacts": bool(self._manual_add_contacts),
            "telemetry_mode_base": int(self._telemetry_mode_base),
            "telemetry_mode_loc":  int(self._telemetry_mode_loc),
            "telemetry_mode_env":  int(self._telemetry_mode_env),
            "adv_loc_policy":      int(self._advert_loc_policy),
            "default_flood_scope": self._default_flood_scope or "",
            "favorites":    sorted(self._favorites),
            "device_info":  self._device_info or {},
            # Full self_info snapshot — radio params, our coordinates, pubkey.
            # Dashboard self-node side panel reads from here.
            "self_info":    {
                "name":           self._self_info_full.get("name", self._self_name),
                "public_key":     self._self_info_full.get("public_key", ""),
                "adv_lat":        self._self_info_full.get("adv_lat", 0.0),
                "adv_lon":        self._self_info_full.get("adv_lon", 0.0),
                "adv_type":       self._self_info_full.get("adv_type", 0),
                "radio_freq":     self._self_info_full.get("radio_freq", 0),
                "radio_bw":       self._self_info_full.get("radio_bw", 0),
                "radio_sf":       self._self_info_full.get("radio_sf", 0),
                "radio_cr":       self._self_info_full.get("radio_cr", 0),
                "tx_power":       self._self_info_full.get("tx_power", 0),
                "max_tx_power":   self._self_info_full.get("max_tx_power", 0),
                "multi_acks":     self._self_info_full.get("multi_acks", 0),
            },
            "self_telemetry": self._self_telemetry or {},
            # Every channel slot the device has, so the dashboard can render
            # an Add/Remove button per slot. Non-empty slots have their
            # `name`; empty slots have name = "".
            "channel_slots": [
                {"idx": i, "name": self._channel_slots.get(i, "")} for i in range(MAX_CHANNEL_SLOTS)
            ],
            "written_at":   int(time.time()),
        }

        plugin_dir    = os.path.dirname(os.path.abspath(__file__))
        domoticz_root = os.path.abspath(os.path.join(plugin_dir, "..", ".."))
        dest = os.path.join(domoticz_root, "www", "templates", "meshcore_devices.json")
        try:
            with open(dest, "w") as f:
                json.dump(payload, f)
        except Exception as exc:
            Domoticz.Debug(f"Could not write device map: {exc}")

    def _write_rx_log(self):
        """Atomically write the rolling RX_LOG_DATA buffer + aggregates as JSON.

        The dashboard fetches this every few seconds to render the analyzer
        detail panel, RF firehose, signal sparklines, packet-rate heatmap,
        duplicate-flood and channel-discovery views.
        """
        plugin_dir    = os.path.dirname(os.path.abspath(__file__))
        domoticz_root = os.path.abspath(os.path.join(plugin_dir, "..", ".."))
        dest = os.path.join(domoticz_root, "www", "templates", "meshcore_rx_log.json")

        now = time.time()
        with self._rx_log_lock:
            entries = list(self._rx_log)
            payload_type_counts = dict(self._payload_type_counts)
            chan_hash_counts    = dict(self._chan_hash_counts)
            signal_history      = {k: list(v) for k, v in self._signal_history.items()}
            dup_floods          = {k: list(v) for k, v in self._dup_floods.items() if len(v) > 1}
            # Trim packet_times to last 24h for the heatmap, then bucket by hour-of-day
            cutoff = now - 86400
            while self._packet_times and self._packet_times[0] < cutoff:
                self._packet_times.popleft()
            heatmap = [0] * 24
            for t in self._packet_times:
                heatmap[time.localtime(t).tm_hour] += 1

        # Build a JSON-safe view of each entry (already normalized in _on_rx_log)
        out_entries = []
        for e in entries:
            row = {}
            for k, v in e.items():
                # Already-normalized values are JSON-safe; defend against stray bytes
                if isinstance(v, (bytes, bytearray)):
                    row[k] = v.hex()
                elif isinstance(v, (str, int, float, bool)) or v is None:
                    row[k] = v
                else:
                    row[k] = str(v)
            out_entries.append(row)

        payload = {
            "written_at": int(now),
            "entries":    out_entries,
            "stats": {
                "payload_type_counts": payload_type_counts,
                "chan_hash_counts":    chan_hash_counts,
                "signal_history":      signal_history,
                "dup_floods":          dup_floods,
                "heatmap_24h":         heatmap,
            },
            "known_channels": self._channel_names,
        }

        try:
            tmp = dest + ".tmp"
            with open(tmp, "w") as f:
                json.dump(payload, f)
            os.replace(tmp, dest)
        except Exception as exc:
            Domoticz.Debug(f"Could not write rx log: {exc}")

    def _remove_custom_page(self):
        plugin_dir    = os.path.dirname(os.path.abspath(__file__))
        domoticz_root = os.path.abspath(os.path.join(plugin_dir, "..", ".."))
        tpl_dir = os.path.join(domoticz_root, "www", "templates")
        fname = "meshcore.html"
        dest = os.path.join(tpl_dir, fname)
        try:
            if os.path.isfile(dest):
                os.remove(dest)
        except Exception as exc:
            Domoticz.Error(f"Failed to remove {fname}: {exc}")
        leaflet_dst = os.path.join(tpl_dir, "leaflet")
        if os.path.isdir(leaflet_dst):
            try:
                import shutil
                shutil.rmtree(leaflet_dst, ignore_errors=True)
            except Exception as exc:
                Domoticz.Debug(f"Failed to remove leaflet dir: {exc}")
        Domoticz.Log("MeshCore dashboard removed.")

    # ── Persistent-connection worker ──────────────────────────────────────────

    def _worker_main(self):
        """Worker thread entry point. Owns an asyncio loop and runs _run() until
        the plugin is stopped. The loop is exposed via self._worker_loop so the
        main thread can schedule sends with asyncio.run_coroutine_threadsafe."""
        Domoticz.Debug("Worker: started")
        loop = asyncio.new_event_loop()
        self._worker_loop = loop
        asyncio.set_event_loop(loop)
        self._stop_async = asyncio.Event()
        self._cmd_lock   = asyncio.Lock()
        try:
            loop.run_until_complete(self._run())
        except asyncio.CancelledError:
            Domoticz.Debug("Worker: cancelled (shutdown).")
        except Exception as exc:
            Domoticz.Error(f"Worker: fatal error: {exc}")
            Domoticz.Debug(traceback.format_exc())
        finally:
            Domoticz.Debug("Worker: entering shutdown sequence")
            try:
                for task in asyncio.all_tasks(loop):
                    task.cancel()
                loop.run_until_complete(asyncio.sleep(0))
            except Exception as exc:
                Domoticz.Debug(f"Worker: cancel-tasks step error: {exc}")
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            except Exception as exc:
                Domoticz.Debug(f"Worker: shutdown_asyncgens error: {exc}")
            try:
                if hasattr(loop, "shutdown_default_executor"):
                    loop.run_until_complete(loop.shutdown_default_executor())
                else:
                    ex = getattr(loop, "_default_executor", None)
                    if ex is not None:
                        ex.shutdown(wait=True)
            except Exception as exc:
                Domoticz.Debug(f"Worker: executor shutdown error: {exc}")
            # On Windows the ProactorEventLoop owns an IOCP that has its own
            # native completion thread (registered as Dummy-N once it calls
            # back into Python). loop.close() is supposed to release it but
            # in practice the thread can linger; closing the proactor first
            # gives the IOCP a chance to flush before the loop tears down.
            try:
                proactor = getattr(loop, "_proactor", None)
                if proactor is not None:
                    proactor.close()
            except Exception as exc:
                Domoticz.Debug(f"Worker: proactor close error: {exc}")
            try:
                loop.close()
            except Exception as exc:
                Domoticz.Debug(f"Worker: loop close error: {exc}")
            self._worker_loop = None
            Domoticz.Debug("Worker: shutdown sequence complete.")

    async def _wait_or_stop(self, secs: float) -> bool:
        """Sleep up to `secs` seconds, returning True if a stop was signalled.

        Uses an asyncio.Event so onStop can wake us instantly via
        call_soon_threadsafe(self._stop_async.set) — no cancellation needed.
        """
        if self._stop_async is None:
            await asyncio.sleep(secs)
            return self._stop_event.is_set()
        try:
            await asyncio.wait_for(self._stop_async.wait(), timeout=secs)
            return True
        except asyncio.TimeoutError:
            return False

    async def _run(self):
        """Outer loop: connect, serve until disconnect, wait, reconnect."""
        while not self._stop_event.is_set():
            try:
                await self._connect_and_serve()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                Domoticz.Debug(f"Worker: serve error: {exc}")
                Domoticz.Debug(traceback.format_exc())

            if self._stop_event.is_set():
                break

            # Wait RECONNECT_DELAY_S before reconnecting (early-exit on stop).
            if await self._wait_or_stop(RECONNECT_DELAY_S):
                return

    async def _connect_and_serve(self):
        """One connection lifecycle: connect, subscribe, serve, disconnect."""
        endpoint = (f"serial {self.serial_port} @ {self.baud_rate}"
                    if self.transport == "Serial"
                    else f"{self.host}:{self.port}")

        try:
            if self.transport == "Serial":
                mc = await asyncio.wait_for(
                    MeshCore.create_serial(self.serial_port, baudrate=self.baud_rate),
                    timeout=CONNECT_TIMEOUT,
                )
            else:
                mc = await asyncio.wait_for(
                    MeshCore.create_tcp(self.host, self.port),
                    timeout=CONNECT_TIMEOUT,
                )
        except Exception as exc:
            if self._was_connected:
                Domoticz.Error(f"Lost connection to MeshCore ({endpoint}): {exc}. Reconnecting in {RECONNECT_DELAY_S}s.")
                self._was_connected = False
            else:
                Domoticz.Error(f"Could not connect to MeshCore ({endpoint}): {exc}. Retrying in {RECONNECT_DELAY_S}s.")
            return

        if mc is None or not mc.is_connected:
            Domoticz.Error(f"MeshCore ({endpoint}): create returned but not connected. Retrying in {RECONNECT_DELAY_S}s.")
            try:
                await self._disconnect_mc(mc)
            except Exception:
                pass
            return

        self._mc = mc
        if not self._was_connected:
            Domoticz.Log(f"Connected to MeshCore ({endpoint}).")
        self._was_connected = True

        # Subscribe to push events. Callbacks run on this loop; they queue
        # work for the main thread via self._queue (no Domoticz API calls!).
        try:
            mc.subscribe(EventType.CONTACT_MSG_RECV, self._on_contact_msg)
            mc.subscribe(EventType.CHANNEL_MSG_RECV, self._on_channel_msg)
            mc.subscribe(EventType.ADVERTISEMENT,    self._on_advertisement)
            mc.subscribe(EventType.RX_LOG_DATA,      self._on_rx_log)
            mc.subscribe(EventType.ACK,              self._on_ack)
        except Exception as exc:
            Domoticz.Error(f"Worker: subscribe failed: {exc}")

        # ── Initial fetches ───────────────────────────────────────────────
        if mc.self_info:
            name = mc.self_info.get("name", "")
            if name:
                self._self_name = name
            self._queue.put(("self_info", dict(mc.self_info)))

        try:
            await self._refresh_contacts(mc)
        except Exception as exc:
            Domoticz.Debug(f"Initial get_contacts error: {exc}")

        if not self._channels_fetched:
            try:
                await self._fetch_channel_names(mc)
                self._channels_fetched = True
            except Exception as exc:
                Domoticz.Debug(f"Initial channel fetch error: {exc}")

        try:
            await self._refresh_flood_scope(mc)
        except Exception as exc:
            Domoticz.Debug(f"Initial flood scope error: {exc}")

        try:
            await self._refresh_device_info(mc)
        except Exception as exc:
            Domoticz.Debug(f"Initial device_info error: {exc}")

        # NOTE: Intentionally NOT calling _drain_push_events(mc) here. Calls
        # to get_msg trigger the same EventType.CONTACT_MSG_RECV /
        # CHANNEL_MSG_RECV events that our push subscriptions consume,
        # producing duplicate inbox entries. The push handlers above are
        # enough — they fire for both newly arriving traffic and any messages
        # the firmware queued before we subscribed.

        try:
            await self._poll_self_stats(mc)
        except Exception as exc:
            Domoticz.Debug(f"Initial self_stats error: {exc}")

        # ── Serve loop ────────────────────────────────────────────────────
        # Wrapped in try/finally so the disconnect always runs — including
        # when the stop event triggers an early return. This is what lets
        # serial_asyncio_fast's executor tasks (open/close) drain so the
        # default thread pool can shut down cleanly on plugin stop.
        try:
            last_stats    = time.monotonic()
            last_contacts = time.monotonic()
            last_rx_write = 0.0

            while not self._stop_event.is_set():
                stopped = await self._wait_or_stop(1.0)
                if stopped:
                    break
                if not mc.is_connected:
                    Domoticz.Error("Worker: connection lost, will reconnect.")
                    break

                now = time.monotonic()

                # Periodic refreshes also acquire the command lock so they
                # don't interleave with user-initiated sends (which would
                # trip the meshcore library's event-subscription race).
                async def _locked(coro):
                    if self._cmd_lock is None:
                        await coro
                        return
                    async with self._cmd_lock:
                        await coro

                if now - last_stats >= STATS_REFRESH_S:
                    last_stats = now
                    try:
                        await _locked(self._refresh_flood_scope(mc))
                    except Exception as exc:
                        Domoticz.Debug(f"Periodic flood_scope error: {exc}")
                    try:
                        await _locked(self._refresh_device_info(mc))
                    except Exception as exc:
                        Domoticz.Debug(f"Periodic device_info error: {exc}")
                    try:
                        await _locked(self._poll_self_stats(mc))
                    except Exception as exc:
                        Domoticz.Debug(f"Periodic self_stats error: {exc}")

                if now - last_contacts >= CONTACTS_REFRESH_S:
                    last_contacts = now
                    try:
                        await _locked(self._refresh_contacts(mc))
                    except Exception as exc:
                        Domoticz.Debug(f"Periodic contacts error: {exc}")

                if self._rx_log_dirty and (now - last_rx_write) >= RX_LOG_WRITE_S:
                    last_rx_write = now
                    self._rx_log_dirty = False
                    # Write directly from the worker thread. _write_rx_log is
                    # pure file I/O + dict copies under self._rx_log_lock; it
                    # doesn't touch the Domoticz API, so it's safe off the
                    # main thread and lets the dashboard see fresh data
                    # without waiting for the next heartbeat tick.
                    self._write_rx_log()
        finally:
            try:
                await self._disconnect_mc(mc)
            except Exception:
                pass
            self._mc = None
            if not self._stop_event.is_set():
                self._was_connected = False

    # ── Push-event callbacks (run on worker loop) ────────────────────────────

    def _on_contact_msg(self, ev):
        self._queue.put(("message", dict(ev.payload or {})))

    def _on_channel_msg(self, ev):
        self._queue.put(("message", dict(ev.payload or {})))

    def _on_advertisement(self, ev):
        self._queue.put(("advert", dict(ev.payload or {})))

    def _on_ack(self, ev):
        """Acknowledgement from a remote node for one of our outbound TEXT_MSGs.

        We don't currently match the ACK back to a specific send (the library
        does that internally for send_msg_with_retry) — but synthesising an
        entry in the rolling RX-log buffer lets the dashboard show ACKs on
        the firehose and gives outbound messages a chance to display an
        "ack received" indicator by timing.
        """
        p = dict(ev.payload or {})
        t = time.time()
        synth = {
            "payload_typename": "ACK",
            "payload_type":     -1,
            "route_typename":   "",
            "snr":              p.get("SNR"),
            "rssi":             p.get("rssi"),
            "recv_time":        int(t),
            "message":          "(acknowledgement)",
            "raw_hex":          "",
            "_t":               t,
        }
        # Preserve any library-supplied identifying field (pkt_hash, code, etc.)
        for k in ("pkt_hash", "code", "ack_code", "request_hash"):
            if k in p:
                synth[k] = p[k]
        with self._rx_log_lock:
            self._rx_log.append(synth)
            self._payload_type_counts["ACK"] = self._payload_type_counts.get("ACK", 0) + 1
            self._packet_times.append(t)
        self._rx_log_dirty = True
        Domoticz.Debug(f"ACK received: {p}")

    def _on_rx_log(self, ev):
        """Record an RX_LOG_DATA event in the rolling buffer + aggregates."""
        p = dict(ev.payload or {})
        # Normalize: timestamps, bytes → hex, enum → name
        t = time.time()
        p["_t"] = t
        # Convert bytes-like fields to hex strings for JSON serialization
        for k, v in list(p.items()):
            if isinstance(v, (bytes, bytearray)):
                p[k] = v.hex()
            elif hasattr(v, "name") and hasattr(v, "value"):  # IntEnum
                p[k] = v.name
        with self._rx_log_lock:
            self._rx_log.append(p)
            # Aggregates
            pt = p.get("payload_typename") or str(p.get("payload_type", ""))
            if pt:
                self._payload_type_counts[pt] = self._payload_type_counts.get(pt, 0) + 1
            ch = p.get("chan_hash")
            if ch:
                self._chan_hash_counts[ch] = self._chan_hash_counts.get(ch, 0) + 1
            self._packet_times.append(t)
            # Per-contact signal history. We key by the first 12 hex chars of
            # the originating contact's public key (a.k.a. pubkey_prefix), so
            # dashboard cards can pull SNR/RSSI history by looking the contact
            # up in the device map. ADVERT carries adv_key directly. For other
            # payload types we can't recover the originating contact from the
            # RX frame alone (the destination is hashed), so they don't show
            # up here — incoming messages will add their own samples via the
            # main-thread _handle_message hook.
            snr  = p.get("snr")
            rssi = p.get("rssi")
            pp   = p.get("path") or ""
            adv_key = p.get("adv_key")
            if p.get("payload_typename") == "ADVERT" and adv_key and (snr is not None or rssi is not None):
                prefix = adv_key[:12]
                hist = self._signal_history.setdefault(prefix, [])
                hist.append({"t": t, "snr": snr, "rssi": rssi, "path_len": p.get("path_len", -1), "kind": "ADVERT"})
                if len(hist) > 60:
                    del hist[: len(hist) - 60]
            # Duplicate-flood detection: keep last few timestamps per raw_hex
            raw = p.get("raw_hex")
            if raw and p.get("route_typename") in ("TC_FLOOD", "FLOOD"):
                dl = self._dup_floods.setdefault(raw, [])
                dl.append({"t": t, "path": pp, "snr": snr})
                if len(dl) > 8:
                    del dl[: len(dl) - 8]
        self._rx_log_dirty = True

    async def _refresh_contacts(self, mc):
        """Issue get_contacts and post the snapshot to the main thread."""
        for attempt in range(3):
            try:
                await asyncio.wait_for(mc.commands.get_contacts(), timeout=COMMAND_TIMEOUT)
            except asyncio.TimeoutError:
                Domoticz.Debug(f"get_contacts timed out (attempt {attempt + 1})")
            except Exception as exc:
                Domoticz.Debug(f"get_contacts error (attempt {attempt + 1}): {exc}")
            await asyncio.sleep(0)
            if mc.contacts:
                break
            await asyncio.sleep(1)
        if mc.contacts:
            self._queue.put(("contacts", {k: dict(v) for k, v in mc.contacts.items()}))

    async def _refresh_flood_scope(self, mc):
        r = await asyncio.wait_for(mc.commands.get_default_flood_scope(), timeout=5.0)
        if r and r.type == EventType.DEFAULT_FLOOD_SCOPE:
            payload = r.payload or {}
            scope_name = payload.get("scope_name", "") or ""
            scope_key  = payload.get("scope_key", "")  or ""
            # The firmware doesn't reliably zero-pad the 31-byte scope_name
            # buffer when overwriting, so leftover bytes from a previous scope
            # bleed back through the meshcore library's NUL-stripping decode.
            # scope_key is the source of truth: it's the sha256-derived
            # routing key. If it's all zeros, the scope is actually empty
            # (global flood). Otherwise trim scope_name to the first run of
            # valid scope characters.
            if scope_key and set(scope_key) <= {"0"}:
                cleaned = ""
            else:
                import re
                m = re.match(r"^([#A-Za-z0-9_\-]+)", scope_name)
                cleaned = m.group(1) if m else ""
            if cleaned != scope_name:
                Domoticz.Debug(
                    f"Flood scope sanitised: raw_name={scope_name!r} "
                    f"key_zero={scope_key and set(scope_key) <= {'0'}} cleaned={cleaned!r}"
                )
            self._queue.put(("flood_scope", cleaned))

    async def _refresh_device_info(self, mc):
        r = await asyncio.wait_for(mc.commands.send_device_query(), timeout=5.0)
        if r and r.type == EventType.DEVICE_INFO:
            self._queue.put(("device_info", dict(r.payload or {})))

    async def _drain_push_events(self, mc):
        """Drain all pending messages from the device using get_msg().

        The device queues incoming messages; we pull them one by one until
        NO_MORE_MSGS is returned.
        """
        fetched = 0
        for _ in range(50):  # safety limit
            try:
                r = await asyncio.wait_for(mc.commands.get_msg(), timeout=5.0)
            except asyncio.TimeoutError:
                break
            except Exception as exc:
                Domoticz.Debug(f"get_msg error: {exc}")
                break
            if r is None or r.type == EventType.NO_MORE_MSGS:
                break
            if r.type in (EventType.CONTACT_MSG_RECV, EventType.CHANNEL_MSG_RECV):
                self._queue.put(("message", r.payload))
                fetched += 1
            elif r.type == EventType.ERROR:
                break
        if fetched:
            Domoticz.Log(f"Fetched {fetched} pending message(s) from device — added to inbox.")

    async def _poll_self_stats(self, mc):
        """Poll all available stats from the connected node itself."""
        Domoticz.Debug("Polling self-node stats...")

        stats = {}

        try:
            r = await asyncio.wait_for(mc.commands.get_stats_core(), timeout=5.0)
            if r and r.type == EventType.STATS_CORE:
                stats.update(r.payload)
                Domoticz.Debug(f"stats_core: {r.payload}")
        except Exception as exc:
            Domoticz.Debug(f"get_stats_core error: {exc}")

        try:
            r = await asyncio.wait_for(mc.commands.get_stats_radio(), timeout=5.0)
            if r and r.type == EventType.STATS_RADIO:
                stats.update(r.payload)
                Domoticz.Debug(f"stats_radio: {r.payload}")
        except Exception as exc:
            Domoticz.Debug(f"get_stats_radio error: {exc}")

        try:
            r = await asyncio.wait_for(mc.commands.get_stats_packets(), timeout=5.0)
            if r and r.type == EventType.STATS_PACKETS:
                stats.update(r.payload)
                Domoticz.Debug(f"stats_packets: {r.payload}")
        except Exception as exc:
            Domoticz.Debug(f"get_stats_packets error: {exc}")

        if stats:
            self._queue.put(("self_stats", stats))

    async def _fetch_channel_names(self, mc):
        """Query all channel slots and record every one (including empty).

        The device map exposes the full slot table so the dashboard can render
        every slot with an Add/Remove control. The legacy meshcore_channels.json
        only contains non-empty entries for backwards compatibility with the
        existing channel resolver.
        """
        channel_names = {}        # non-empty only — used by message routing
        all_slots: dict = {}      # idx → name ("" if empty) for all slots
        for idx in range(MAX_CHANNEL_SLOTS):
            try:
                res = await asyncio.wait_for(mc.commands.get_channel(idx), timeout=2.0)
                Domoticz.Debug(f"get_channel({idx}): type={res.type if res else None} payload={res.payload if res else None}")
                if res and res.type == EventType.CHANNEL_INFO:
                    name = res.payload.get("channel_name", "").strip("\x00").strip()
                    all_slots[idx] = name
                    if name:
                        channel_names[str(idx)] = name
                elif res and res.type == EventType.ERROR:
                    # ERROR usually means firmware reports no such slot —
                    # record the remaining slots as empty and stop probing.
                    for j in range(idx, MAX_CHANNEL_SLOTS):
                        all_slots.setdefault(j, "")
                    break
            except asyncio.TimeoutError:
                Domoticz.Debug(f"get_channel({idx}) timed out — assume empty")
                all_slots[idx] = ""
                continue
            except Exception as exc:
                Domoticz.Debug(f"get_channel({idx}) error: {exc} — assume empty")
                all_slots[idx] = ""
                continue
        # Ensure full coverage
        for j in range(MAX_CHANNEL_SLOTS):
            all_slots.setdefault(j, "")
        self._channel_slots = all_slots
        self._channel_names = {int(k): v for k, v in channel_names.items()}
        if channel_names:
            parts = [f"#{k} = {v}" for k, v in sorted(channel_names.items())]
            Domoticz.Log(f"MeshCore channels: {', '.join(parts)}")
        else:
            Domoticz.Debug("No channel names found on device.")
        self._write_channel_names(channel_names)

    async def _run_remote_query(self, verb: str, name: str, coro, kind: str):
        """Run a req_*_sync call as a detached task and queue the result.

        Detaches the slow remote round-trip from the main send pipeline so an
        unresponsive contact can't stall other sends or periodic pollers.
        Tasks are bounded to 30s. Held under the global command lock to avoid
        the meshcore library's per-call event subscription race.
        """
        lock = self._cmd_lock
        try:
            if lock is not None:
                await lock.acquire()
            r = await asyncio.wait_for(coro, timeout=30.0)
            ok = r is not None and getattr(r, "type", None) != EventType.ERROR
            if ok:
                payload = dict(r.payload or {}) if hasattr(r, "payload") else None
                # neighbours payload may be a list — preserve as-is
                if r.payload is not None and not isinstance(r.payload, dict):
                    payload = r.payload
                self._queue.put((kind, {"name": name, "data": payload}))
            self._queue.put(("send_result", {
                "ok": ok, "target": verb, "body": name,
                "result": "received" if ok else "no response from remote",
            }))
        except asyncio.TimeoutError:
            self._queue.put(("send_result", {
                "ok": False, "target": verb, "body": name,
                "result": "timeout — remote did not respond within 30s",
            }))
        except Exception as exc:
            self._queue.put(("send_result", {
                "ok": False, "target": verb, "body": name, "result": str(exc),
            }))
        finally:
            try:
                if lock is not None and lock.locked():
                    lock.release()
            except RuntimeError:
                pass

    async def _send_message_for_text(self, text: str):
        """Wrapper invoked via run_coroutine_threadsafe by onDeviceModified.

        Reads the live mc instance inside the worker loop and short-circuits
        with a friendly result if we are mid-reconnect — the alternative is
        a stale mc reference that would error confusingly inside the library.

        Holds the global command lock so concurrent rapid-fire sends (e.g.
        applying a preset that issues 4 verbs back-to-back) don't trip the
        meshcore library's per-call event-subscription race condition.
        """
        mc = self._mc
        if mc is None or not getattr(mc, "is_connected", False):
            self._queue.put(("send_result", {
                "ok": False, "target": "?", "body": text,
                "result": "not connected — auto-reconnect in progress",
            }))
            return
        lock = self._cmd_lock
        if lock is None:
            await self._send_message(mc, text)
            return
        async with lock:
            await self._send_message(mc, text)

    async def _send_message(self, mc, text: str):
        """Send a message from the Mesh Send device value.

        Syntax accepted:
          "hello world"          → direct message to the first tracked node
          "garden: hello"        → direct message to the node named 'garden'
          "#0: hello"            → broadcast on channel index 0
          "#General: hello"      → broadcast on the channel named 'General'
          "#flood: hello"        → broadcast on channel 0 (alias)
          "!remove <name>"       → remove the named contact from the device
        """
        # Note: "!favorite ..." is intentionally NOT handled here — it is consumed
        # locally by onDeviceModified()/_handle_local_only_command() without
        # opening an MC session, since the operation only touches plugin state.

        # ── Remote-contact verbs ─────────────────────────────────────────
        # Helper: resolve a contact dict by adv_name from mc.contacts.
        def _resolve_contact(name):
            for c in mc.contacts.values():
                if c.get("adv_name", "").strip() == name:
                    return dict(c)
            return None

        # !reset_path <contact name>
        if text.startswith("!reset_path "):
            name = text[len("!reset_path "):].strip()
            contact = _resolve_contact(name)
            if contact is None:
                self._queue.put(("send_result", {
                    "ok": False, "target": "!reset_path", "body": name,
                    "result": f"contact '{name}' not found"
                }))
                return
            try:
                # reset_path takes a pubkey-like key
                pk = bytes.fromhex(contact.get("public_key", ""))
                r = await asyncio.wait_for(mc.commands.reset_path(pk), timeout=8.0)
                ok = r is not None and r.type == EventType.OK
                if ok:
                    Domoticz.Log(f"Path reset for '{name}'")
                self._queue.put(("send_result", {
                    "ok": ok, "target": "!reset_path", "body": name,
                    "result": "applied" if ok else str(r),
                }))
            except Exception as exc:
                self._queue.put(("send_result", {
                    "ok": False, "target": "!reset_path", "body": name, "result": str(exc),
                }))
            return

        # !req_status / !req_telemetry / !req_neighbours <contact name>
        # Remote queries can block for tens of seconds while waiting for the
        # remote node to respond. We run them as detached asyncio tasks so a
        # slow / offline contact doesn't pin the worker loop and stall other
        # sends or the periodic pollers.
        for verb, fn_name, kind, ok_event_only in (
            ("!req_status ",     "req_status_sync",     "contact_status",     False),
            ("!req_telemetry ",  "req_telemetry_sync",  "contact_telemetry",  False),
            ("!req_neighbours ", "req_neighbours_sync", "contact_neighbours", False),
        ):
            if text.startswith(verb):
                name = text[len(verb):].strip()
                contact = _resolve_contact(name)
                if contact is None:
                    self._queue.put(("send_result", {
                        "ok": False, "target": verb.strip(), "body": name,
                        "result": f"contact '{name}' not found"
                    }))
                    return
                asyncio.create_task(self._run_remote_query(
                    verb.strip(), name, getattr(mc.commands, fn_name)(contact), kind
                ))
                # Immediate optimistic ack so the UI doesn't sit on a spinner
                self._queue.put(("send_result", {
                    "ok": True, "target": verb.strip(), "body": name,
                    "result": "querying (up to 30s)",
                }))
                return

        # ── Self-node verbs ──────────────────────────────────────────────
        # !send_advert [direct|flood]   default flood
        if text.startswith("!send_advert"):
            arg = text[len("!send_advert"):].strip().lower()
            flood = arg != "direct"   # anything other than "direct" → flood
            try:
                r = await asyncio.wait_for(mc.commands.send_advert(flood=flood), timeout=8.0)
                ok = r is not None and r.type == EventType.OK
                if ok:
                    Domoticz.Log(f"Advertisement sent ({'flood' if flood else 'direct'}).")
                self._queue.put(("send_result", {
                    "ok": ok, "target": "!send_advert",
                    "body": "flood" if flood else "direct",
                    "result": "applied" if ok else str(r),
                }))
            except Exception as exc:
                self._queue.put(("send_result", {
                    "ok": False, "target": "!send_advert", "body": arg, "result": str(exc),
                }))
            return

        # !set_radio <freq_MHz> <bw_kHz> <sf 7-12> <cr 5-8>
        if text.startswith("!set_radio"):
            parts = text.split()
            if len(parts) != 5:
                self._queue.put(("send_result", {
                    "ok": False, "target": "!set_radio", "body": text,
                    "result": "syntax: !set_radio <freq MHz> <bw kHz> <sf 7-12> <cr 5-8>"
                }))
                return
            try:
                freq, bw, sf, cr = float(parts[1]), float(parts[2]), int(parts[3]), int(parts[4])
            except ValueError as exc:
                self._queue.put(("send_result", {
                    "ok": False, "target": "!set_radio", "body": text, "result": f"parse error: {exc}"
                }))
                return
            # Sanity bounds. ISM bands cover 100-2500 MHz; MeshCore typically
            # runs 433 / 868 / 915 MHz. SF is 7-12, CR is 5-8 (= 4/5..4/8).
            # Wide bandwidth range to allow narrow-band experimentation.
            if not (100.0 <= freq <= 2500.0):
                self._queue.put(("send_result", {
                    "ok": False, "target": "!set_radio", "body": text,
                    "result": "freq must be 100-2500 MHz"
                }))
                return
            if not (5.0 <= bw <= 500.0):
                self._queue.put(("send_result", {
                    "ok": False, "target": "!set_radio", "body": text,
                    "result": "bw must be 5-500 kHz"
                }))
                return
            if not (7 <= sf <= 12):
                self._queue.put(("send_result", {
                    "ok": False, "target": "!set_radio", "body": text,
                    "result": "sf must be 7-12"
                }))
                return
            if not (5 <= cr <= 8):
                self._queue.put(("send_result", {
                    "ok": False, "target": "!set_radio", "body": text,
                    "result": "cr must be 5-8 (=4/5..4/8)"
                }))
                return
            try:
                r = await asyncio.wait_for(mc.commands.set_radio(freq, bw, sf, cr), timeout=8.0)
                ok = r is not None and r.type == EventType.OK
                if ok:
                    Domoticz.Log(f"Radio set: freq={freq} MHz bw={bw} kHz sf={sf} cr={cr}")
                    # Optimistically update local snapshot — firmware will re-emit
                    # SELF_INFO on next connect / advert anyway.
                    self._self_info_full["radio_freq"] = freq
                    self._self_info_full["radio_bw"]   = bw
                    self._self_info_full["radio_sf"]   = sf
                    self._self_info_full["radio_cr"]   = cr
                    self._write_device_map()
                self._queue.put(("send_result", {
                    "ok": ok, "target": "!set_radio",
                    "body": f"{freq}/{bw}/sf{sf}/cr{cr}",
                    "result": "applied" if ok else str(r),
                }))
            except Exception as exc:
                self._queue.put(("send_result", {
                    "ok": False, "target": "!set_radio", "body": text, "result": str(exc),
                }))
            return

        # !set_tx_power <dBm>
        if text.startswith("!set_tx_power"):
            parts = text.split()
            if len(parts) != 2:
                self._queue.put(("send_result", {
                    "ok": False, "target": "!set_tx_power", "body": text,
                    "result": "syntax: !set_tx_power <dBm>"
                }))
                return
            try:
                p = int(parts[1])
            except ValueError as exc:
                self._queue.put(("send_result", {
                    "ok": False, "target": "!set_tx_power", "body": text, "result": str(exc),
                }))
                return
            # Clamp against the firmware-reported maximum to avoid bricking
            # the radio at hardware-illegal levels. Fall back to a generous
            # 22 dBm ceiling when max isn't known yet.
            max_tx = int(self._self_info_full.get("max_tx_power", 22) or 22)
            if not (0 <= p <= max_tx):
                self._queue.put(("send_result", {
                    "ok": False, "target": "!set_tx_power", "body": str(p),
                    "result": f"tx_power must be 0-{max_tx} dBm",
                }))
                return
            try:
                r = await asyncio.wait_for(mc.commands.set_tx_power(p), timeout=8.0)
                ok = r is not None and r.type == EventType.OK
                if ok:
                    Domoticz.Log(f"TX power set to {p} dBm")
                    self._self_info_full["tx_power"] = p
                    self._write_device_map()
                self._queue.put(("send_result", {
                    "ok": ok, "target": "!set_tx_power", "body": str(p),
                    "result": "applied" if ok else str(r),
                }))
            except Exception as exc:
                self._queue.put(("send_result", {
                    "ok": False, "target": "!set_tx_power", "body": text, "result": str(exc),
                }))
            return

        # !set_name <new name>
        if text.startswith("!set_name "):
            new_name = text[len("!set_name "):].strip()
            if not new_name:
                self._queue.put(("send_result", {
                    "ok": False, "target": "!set_name", "body": text,
                    "result": "name must not be empty"
                }))
                return
            # Defend in depth: HTML caps at 32 but the /json.htm endpoint
            # bypasses that. Reject overlong / non-printable names.
            if len(new_name) > 32:
                self._queue.put(("send_result", {
                    "ok": False, "target": "!set_name", "body": new_name,
                    "result": "name must be ≤ 32 characters"
                }))
                return
            if not all(c.isprintable() for c in new_name):
                self._queue.put(("send_result", {
                    "ok": False, "target": "!set_name", "body": new_name,
                    "result": "name contains non-printable characters"
                }))
                return
            try:
                r = await asyncio.wait_for(mc.commands.set_name(new_name), timeout=8.0)
                ok = r is not None and r.type == EventType.OK
                if ok:
                    Domoticz.Log(f"Device name set to: {new_name}")
                    self._self_name = new_name
                    self._self_info_full["name"] = new_name
                    self._write_device_map()
                self._queue.put(("send_result", {
                    "ok": ok, "target": "!set_name", "body": new_name,
                    "result": "applied" if ok else str(r),
                }))
            except Exception as exc:
                self._queue.put(("send_result", {
                    "ok": False, "target": "!set_name", "body": new_name, "result": str(exc),
                }))
            return

        # !set_coords <lat> <lon>
        if text.startswith("!set_coords"):
            parts = text.split()
            if len(parts) != 3:
                self._queue.put(("send_result", {
                    "ok": False, "target": "!set_coords", "body": text,
                    "result": "syntax: !set_coords <lat> <lon>"
                }))
                return
            try:
                lat, lon = float(parts[1]), float(parts[2])
            except ValueError as exc:
                self._queue.put(("send_result", {
                    "ok": False, "target": "!set_coords", "body": text, "result": str(exc),
                }))
                return
            if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
                self._queue.put(("send_result", {
                    "ok": False, "target": "!set_coords", "body": f"{lat},{lon}",
                    "result": "lat must be -90..90 and lon must be -180..180",
                }))
                return
            try:
                r = await asyncio.wait_for(mc.commands.set_coords(lat, lon), timeout=8.0)
                ok = r is not None and r.type == EventType.OK
                if ok:
                    Domoticz.Log(f"Coords set: lat={lat} lon={lon}")
                    self._self_info_full["adv_lat"] = lat
                    self._self_info_full["adv_lon"] = lon
                    self._write_device_map()
                self._queue.put(("send_result", {
                    "ok": ok, "target": "!set_coords", "body": f"{lat},{lon}",
                    "result": "applied" if ok else str(r),
                }))
            except Exception as exc:
                self._queue.put(("send_result", {
                    "ok": False, "target": "!set_coords", "body": text, "result": str(exc),
                }))
            return

        # !set_path_hash_mode <1|2|3>
        #   1 = 1-byte hashes (max ~64 hops)
        #   2 = 2-byte hashes (max ~32 hops)
        #   3 = 3-byte hashes (max ~21 hops, default on SF8)
        if text.startswith("!set_path_hash_mode"):
            parts = text.split()
            if len(parts) != 2:
                self._queue.put(("send_result", {
                    "ok": False, "target": "!set_path_hash_mode", "body": text,
                    "result": "syntax: !set_path_hash_mode <1|2|3>"
                }))
                return
            try:
                mode = int(parts[1])
                if mode < 1 or mode > 3:
                    raise ValueError("mode must be 1, 2 or 3")
            except ValueError as exc:
                self._queue.put(("send_result", {
                    "ok": False, "target": "!set_path_hash_mode", "body": text, "result": str(exc),
                }))
                return
            # Wire format: firmware uses 0/1/2 to mean 1/2/3-byte hashes
            # (off-by-one vs. the human-friendly value the UI uses). Translate
            # at the boundary so the rest of the code can stay in 1/2/3.
            wire_mode = mode - 1
            try:
                r = await asyncio.wait_for(mc.commands.set_path_hash_mode(wire_mode), timeout=8.0)
                ok = r is not None and r.type == EventType.OK
                if ok:
                    Domoticz.Log(f"Path hash mode set to {mode}-byte (wire={wire_mode})")
                    # Store the human-friendly value so the dashboard dropdown
                    # selects the same option the user picked.
                    self._device_info["path_hash_mode"] = mode
                    self._write_device_map()
                self._queue.put(("send_result", {
                    "ok": ok, "target": "!set_path_hash_mode", "body": str(mode),
                    "result": "applied" if ok else str(r),
                }))
            except Exception as exc:
                self._queue.put(("send_result", {
                    "ok": False, "target": "!set_path_hash_mode", "body": text, "result": str(exc),
                }))
            return

        # !reboot
        if text.startswith("!reboot"):
            try:
                # reboot doesn't wait for a response — the device acks then
                # disconnects roughly 1s later. We only swallow disconnect-
                # class exceptions (TimeoutError, ConnectionError) as expected;
                # other errors (e.g. permission denied, bad state) are reported.
                await asyncio.wait_for(mc.commands.reboot(), timeout=5.0)
                Domoticz.Log("Reboot command sent.")
                self._queue.put(("send_result", {
                    "ok": True, "target": "!reboot", "body": "", "result": "sent",
                }))
            except (asyncio.TimeoutError, ConnectionError, OSError) as exc:
                # Disconnection during reboot is expected — the device just
                # reset and our serial/TCP link dropped. Treat as success.
                Domoticz.Log(f"Reboot sent (device disconnected as expected: {exc})")
                self._queue.put(("send_result", {
                    "ok": True, "target": "!reboot", "body": "", "result": "sent",
                }))
            except Exception as exc:
                Domoticz.Error(f"Reboot failed: {exc}")
                self._queue.put(("send_result", {
                    "ok": False, "target": "!reboot", "body": "", "result": str(exc),
                }))
            return

        # !get_telemetry — local sensors
        if text.startswith("!get_telemetry"):
            try:
                r = await asyncio.wait_for(mc.commands.get_self_telemetry(), timeout=8.0)
                payload = dict(r.payload or {}) if r else {}
                ok = r is not None
                if ok:
                    self._queue.put(("self_telemetry", payload))
                self._queue.put(("send_result", {
                    "ok": ok, "target": "!get_telemetry", "body": "",
                    "result": "received" if ok else "no response",
                }))
            except Exception as exc:
                self._queue.put(("send_result", {
                    "ok": False, "target": "!get_telemetry", "body": "", "result": str(exc),
                }))
            return

        # ── Special: set / clear a channel slot ─────────────────────────────
        # Syntax:
        #   !set_channel <slot> <name>            — name starting with '#'
        #                                            auto-derives the secret
        #   !set_channel <slot> <name> <secret>   — explicit 32-hex-char secret
        #   !clear_channel <slot>                 — wipe slot to empty
        if text.startswith("!set_channel"):
            parts = text.split(None, 3)
            if len(parts) < 3:
                self._queue.put(("send_result", {
                    "ok": False, "target": "!set_channel", "body": text,
                    "result": f"syntax: !set_channel <slot 0-{MAX_CHANNEL_SLOTS-1}> <name> [secret_hex]"
                }))
                return
            try:
                slot = int(parts[1])
                if slot < 0 or slot >= MAX_CHANNEL_SLOTS:
                    raise ValueError(f"slot must be 0..{MAX_CHANNEL_SLOTS-1}")
            except ValueError as exc:
                self._queue.put(("send_result", {
                    "ok": False, "target": "!set_channel", "body": text,
                    "result": f"bad slot: {exc}"
                }))
                return
            name = parts[2]
            # Names beginning with '!' would re-enter the local command parser
            # on the next plugin restart if anything echoed them back, and
            # they're not valid MeshCore channel names anyway.
            if name.startswith("!"):
                self._queue.put(("send_result", {
                    "ok": False, "target": "!set_channel", "body": text,
                    "result": "channel name must not start with '!'"
                }))
                return
            secret = None
            if len(parts) >= 4:
                try:
                    secret = bytes.fromhex(parts[3])
                except ValueError as exc:
                    self._queue.put(("send_result", {
                        "ok": False, "target": "!set_channel", "body": text,
                        "result": f"bad secret hex: {exc}"
                    }))
                    return
                if len(secret) != 16:
                    self._queue.put(("send_result", {
                        "ok": False, "target": "!set_channel", "body": text,
                        "result": "secret must be exactly 16 bytes (32 hex chars)"
                    }))
                    return
            try:
                result = await asyncio.wait_for(
                    mc.commands.set_channel(slot, name, secret), timeout=10.0
                )
                ok = result is not None and result.type == EventType.OK
                if ok:
                    # Refresh slot table immediately so the dashboard sees it
                    await self._fetch_channel_names(mc)
                    self._write_device_map()
                    Domoticz.Log(f"Channel slot {slot} set to '{name}'")
                self._queue.put(("send_result", {
                    "ok": ok, "target": "!set_channel",
                    "body": f"slot={slot} name={name}",
                    "result": "applied" if ok else str(result),
                }))
            except Exception as exc:
                self._queue.put(("send_result", {
                    "ok": False, "target": "!set_channel",
                    "body": f"slot={slot} name={name}", "result": str(exc),
                }))
            return

        if text.startswith("!clear_channel"):
            parts = text.split(None, 1)
            if len(parts) < 2:
                self._queue.put(("send_result", {
                    "ok": False, "target": "!clear_channel", "body": text,
                    "result": f"syntax: !clear_channel <slot 0-{MAX_CHANNEL_SLOTS-1}>"
                }))
                return
            try:
                slot = int(parts[1])
                if slot < 0 or slot >= MAX_CHANNEL_SLOTS:
                    raise ValueError(f"slot must be 0..{MAX_CHANNEL_SLOTS-1}")
            except ValueError as exc:
                self._queue.put(("send_result", {
                    "ok": False, "target": "!clear_channel", "body": text,
                    "result": f"bad slot: {exc}"
                }))
                return
            try:
                # Clear by writing an empty name + zero secret. The library
                # accepts any 16-byte secret; with empty name the firmware
                # treats the slot as free.
                result = await asyncio.wait_for(
                    mc.commands.set_channel(slot, "", b"\x00" * 16), timeout=10.0
                )
                ok = result is not None and result.type == EventType.OK
                if ok:
                    await self._fetch_channel_names(mc)
                    self._write_device_map()
                    Domoticz.Log(f"Channel slot {slot} cleared.")
                self._queue.put(("send_result", {
                    "ok": ok, "target": "!clear_channel",
                    "body": f"slot={slot}",
                    "result": "cleared" if ok else str(result),
                }))
            except Exception as exc:
                self._queue.put(("send_result", {
                    "ok": False, "target": "!clear_channel",
                    "body": f"slot={slot}", "result": str(exc),
                }))
            return

        # ── Special: set default flood scope ───────────────────────────────
        # Syntax: "!flood_scope <name>"  (empty name = reset to global flood)
        if text.startswith("!flood_scope"):
            arg = text[len("!flood_scope"):].strip()
            # Library treats "", "0", "None", "*" as reset. We pass the empty
            # string for reset (NOT None) because meshcore.commands.messaging
            # has a bug where set_default_flood_scope(None) calls len(scope)
            # on the still-None argument. Empty string takes the elif branch
            # and resets correctly.
            scope_to_set = arg or ""
            # Mirror the library's disable sentinels so our UI/state stays in
            # sync. "0", "*", "None" all mean "clear scope" — collapse to "".
            if scope_to_set in ("0", "*", "None"):
                scope_to_set = ""
            try:
                # When clearing, also clear the runtime scope first. The firmware
                # refuses set_default_flood_scope("") while a runtime scope is
                # active (returns ERR_CODE_ILLEGAL_ARG). Clearing the runtime
                # scope first removes that block.
                if not scope_to_set:
                    try:
                        await asyncio.wait_for(
                            mc.commands.set_flood_scope(""), timeout=10.0
                        )
                    except Exception as e:
                        Domoticz.Debug(f"set_flood_scope clear pre-step failed: {e}")
                    # Try each disable sentinel in turn: "0", "", "*".
                    result = None
                    for sentinel in ("0", "", "*"):
                        try:
                            result = await asyncio.wait_for(
                                mc.commands.set_default_flood_scope(sentinel),
                                timeout=10.0,
                            )
                        except Exception as e:
                            Domoticz.Debug(f"set_default_flood_scope({sentinel!r}) raised: {e}")
                            continue
                        if result is not None and result.type == EventType.OK:
                            Domoticz.Debug(f"Empty-scope clear accepted with sentinel {sentinel!r}")
                            break
                else:
                    result = await asyncio.wait_for(
                        mc.commands.set_default_flood_scope(scope_to_set), timeout=10.0
                    )
                ok = result is not None and result.type == EventType.OK
                if ok:
                    # Normalize the stored value to what the device would echo back
                    if not scope_to_set:
                        self._default_flood_scope = ""
                    else:
                        s = scope_to_set
                        if not s.startswith("#"):
                            s = "#" + s
                        self._default_flood_scope = s
                    Domoticz.Log(f"Default flood scope set to {self._default_flood_scope or '(none)'}")
                    self._write_device_map()
                # Detect the firmware's "ILLEGAL_ARG on empty-scope set" quirk
                # and surface a user-facing hint about !reboot. The firmware
                # refuses to wipe an active scope_key in-place; it only clears
                # on power-on. Reproduces consistently on fw 27 (Apr 2026).
                err_payload = (result.payload if result is not None else None) or {}
                is_empty_scope_quirk = (
                    not ok and not scope_to_set
                    and isinstance(err_payload, dict)
                    and err_payload.get("code_string") == "ERR_CODE_ILLEGAL_ARG"
                )
                if is_empty_scope_quirk:
                    result_str = ("firmware refused empty-scope set while a scope is active. "
                                  "Workaround: clear scope via the MeshCore phone app and save, "
                                  "or send !reboot — the scope clears on startup.")
                else:
                    result_str = "applied" if ok else str(result)
                self._queue.put(("send_result", {
                    "ok": ok, "target": "!flood_scope",
                    "body": arg or "(reset)",
                    "result": result_str,
                }))
            except Exception as exc:
                self._queue.put(("send_result", {
                    "ok": False, "target": "!flood_scope",
                    "body": arg or "(reset)", "result": str(exc),
                }))
            return

        # ── Special: set device parameter ──────────────────────────────────
        # Syntax: "!set <key> <int-value>"
        # Supported keys: telemetry_base, telemetry_loc, telemetry_env, adv_loc_policy
        if text.startswith("!set "):
            try:
                _, key, val = text.split(None, 2)
                ival = int(val)
            except ValueError:
                self._queue.put(("send_result", {"ok": False, "target": "!set", "body": text, "result": "syntax: !set <key> <int>"}))
                return
            cmd_map = {
                "telemetry_base":  mc.commands.set_telemetry_mode_base,
                "telemetry_loc":   mc.commands.set_telemetry_mode_loc,
                "telemetry_env":   mc.commands.set_telemetry_mode_env,
                "adv_loc_policy":  mc.commands.set_advert_loc_policy,
            }
            fn = cmd_map.get(key)
            if fn is None:
                self._queue.put(("send_result", {"ok": False, "target": "!set", "body": text, "result": f"unknown key '{key}'"}))
                return
            try:
                result = await asyncio.wait_for(fn(ival), timeout=10.0)
                ok = result is not None and result.type == EventType.OK
                if ok:
                    if key == "telemetry_base":     self._telemetry_mode_base = ival
                    elif key == "telemetry_loc":    self._telemetry_mode_loc = ival
                    elif key == "telemetry_env":    self._telemetry_mode_env = ival
                    elif key == "adv_loc_policy":   self._advert_loc_policy = ival
                    self._settings_set_at = time.monotonic()
                    Domoticz.Log(f"Device {key} = {ival}")
                self._queue.put(("send_result", {
                    "ok": ok, "target": "!set", "body": f"{key}={ival}",
                    "result": "applied" if ok else str(result),
                }))
            except Exception as exc:
                self._queue.put(("send_result", {"ok": False, "target": "!set", "body": f"{key}={ival}", "result": str(exc)}))
            return

        # ── Special: toggle manual_add_contacts on the connected node ──────
        if text.startswith("!manual_add"):
            arg = text[len("!manual_add"):].strip().lower()
            enable = arg in ("on", "1", "true", "yes")
            try:
                result = await asyncio.wait_for(
                    mc.commands.set_manual_add_contacts(enable), timeout=10.0
                )
                ok = result is not None and result.type == EventType.OK
                if ok:
                    self._manual_add_contacts = enable
                    self._settings_set_at = time.monotonic()
                    Domoticz.Log(f"manual_add_contacts set to {enable} on device.")
                self._queue.put(("send_result", {
                    "ok": ok,
                    "target": "!manual_add",
                    "body": "on" if enable else "off",
                    "result": "applied" if ok else str(result),
                }))
            except Exception as exc:
                self._queue.put(("send_result", {
                    "ok": False, "target": "!manual_add",
                    "body": "on" if enable else "off", "result": str(exc),
                }))
            return

        # ── Special: remove contact ────────────────────────────────────────
        if text.startswith("!remove "):
            name = text[len("!remove "):].strip()
            if not name:
                self._queue.put(("send_result", {"ok": False, "target": "!remove", "body": text, "result": "no contact name"}))
                return
            contact = None
            for c in mc.contacts.values():
                if c.get("adv_name", "").strip() == name:
                    contact = dict(c)
                    break
            if contact is None:
                self._queue.put(("send_result", {"ok": False, "target": "!remove", "body": text, "result": f"contact '{name}' not found"}))
                return
            try:
                result = await asyncio.wait_for(mc.commands.remove_contact(contact), timeout=10.0)
                ok = result is not None and result.type == EventType.OK
                self._queue.put(("send_result", {
                    "ok": ok,
                    "target": "!remove",
                    "body": name,
                    "result": "removed" if ok else str(result),
                }))
                if ok:
                    # Drop from local tracking so it disappears from the dashboard immediately
                    if name in self._contact_names:
                        self._contact_names.remove(name)
                    self._node_types.pop(name, None)
                    self._node_last_advert.pop(name, None)
                    self._node_pubkey.pop(name, None)
                    self._contact_query_results.pop(name, None)
                    self._node_last_activity.pop(name, None)
                    self._node_locations.pop(name, None)
                    if name in self._favorites:
                        self._favorites.discard(name)
                        self._save_favorites()
            except Exception as exc:
                self._queue.put(("send_result", {"ok": False, "target": "!remove", "body": name, "result": str(exc)}))
            return

        target = None
        body   = text

        if ":" in text:
            prefix, rest = text.split(":", 1)
            prefix = prefix.strip()
            body   = rest.strip()
            if prefix.startswith("#"):
                chan_part = prefix[1:].strip()
                if chan_part.lower() in ("", "flood"):
                    chan_idx = 0
                elif chan_part.isdigit():
                    chan_idx = int(chan_part)
                else:
                    # Resolve channel name → index (case-insensitive)
                    chan_idx = None
                    for idx, name in self._channel_names.items():
                        if name.lower() == chan_part.lower():
                            chan_idx = idx
                            break
                    if chan_idx is None:
                        self._queue.put(("send_result", {"ok": False, "target": prefix,
                                                         "body": body, "result": f"Unknown channel name '{chan_part}'. Known: {self._channel_names}"}))
                        return
                try:
                    result = await asyncio.wait_for(
                        mc.commands.send_chan_msg(chan_idx, body), timeout=15.0
                    )
                    tx_busy = (
                        result is not None
                        and result.type == EventType.ERROR
                        and (result.payload or {}).get("reason") == "no_event_received"
                    )
                    ok = result is not None and result.type == EventType.OK
                    self._queue.put(("send_result", {"ok": ok, "target": f"#{chan_idx}", "body": body,
                                                    "result": "TX busy — try again" if tx_busy else str(result)}))
                except Exception as exc:
                    self._queue.put(("send_result", {"ok": False, "target": f"#{chan_idx}", "body": body, "result": str(exc)}))
                return
            else:
                target = prefix  # node name

        # Direct message to a node — success response is EventType.MSG_SENT
        if target is None:
            target = self._contact_names[0] if self._contact_names else ""
        if not target:
            self._queue.put(("send_result", {"ok": False, "target": target, "body": body, "result": "no target node"}))
            return

        contact = None
        for c in mc.contacts.values():
            if c.get("adv_name", "").strip() == target:
                contact = dict(c)
                break

        if contact is None:
            self._queue.put(("send_result", {"ok": False, "target": target, "body": body, "result": "contact not found"}))
            return

        # Use plain send_msg: returns as soon as the local node has accepted
        # the packet for TX. We don't wait for the destination ACK here —
        # send_msg_with_retry's 40-60s wait was too painful in practice for
        # cases where the recipient is offline. (If you want delivered/no-ACK
        # status, the path is to switch back to send_msg_with_retry or wire
        # up a non-blocking background ACK listener.)
        try:
            result = await asyncio.wait_for(
                mc.commands.send_msg(contact, body), timeout=15.0
            )
            tx_busy = (
                result is not None
                and result.type == EventType.ERROR
                and (result.payload or {}).get("reason") == "no_event_received"
            )
            ok = result is not None and result.type == EventType.MSG_SENT
            self._queue.put(("send_result", {"ok": ok, "target": target, "body": body,
                                             "result": "TX busy — try again" if tx_busy else str(result)}))
        except Exception as exc:
            self._queue.put(("send_result", {"ok": False, "target": target, "body": body, "result": str(exc)}))

    # ── Queue dispatcher (runs on Domoticz main thread via onHeartbeat) ───────

    def _dispatch(self, item):
        kind = item[0]
        if kind == "message":
            Domoticz.Debug(f"Message: {item[1]}")
            self._handle_message(item[1])
        elif kind == "advert":
            # Ambient advertisement — used by handlers that want a hint that
            # a node is alive even before its first message. We don't update
            # any Domoticz device from this directly; the next contacts refresh
            # handles status/last_advert. Keeping the hook so dashboards can
            # tap into it later via the device map's last_advert field.
            pass
        elif kind == "contacts":
            self._handle_contacts(item[1])
            # First contacts batch processed — bump heartbeat back to a
            # steady 10s cadence. Push events still arrive instantly via
            # the worker thread; the heartbeat is only there to drain the
            # cross-thread queue. 10s feels live without thrashing.
            if not self._heartbeat_restored:
                Domoticz.Heartbeat(10)
                self._heartbeat_restored = True
        elif kind == "self_stats":
            self._handle_self_stats(item[1])
        elif kind == "self_telemetry":
            # Latest local-sensor reading from get_self_telemetry. Stored in
            # the device map so the self-node side panel can render it.
            self._self_telemetry = item[1] or {}
            self._write_device_map()
        elif kind == "contact_status":
            d = item[1] or {}
            name = d.get("name")
            if name:
                self._contact_query_results.setdefault(name, {})["status"] = {
                    "t": int(time.time()), "data": d.get("data") or {}
                }
                self._write_device_map()
        elif kind == "contact_telemetry":
            d = item[1] or {}
            name = d.get("name")
            if name:
                self._contact_query_results.setdefault(name, {})["telemetry"] = {
                    "t": int(time.time()), "data": d.get("data") or {}
                }
                self._write_device_map()
        elif kind == "contact_neighbours":
            d = item[1] or {}
            name = d.get("name")
            if name:
                # Neighbours payload can be a list — preserve as-is, JSON serializer
                # will handle it.
                data = d.get("data")
                self._contact_query_results.setdefault(name, {})["neighbours"] = {
                    "t": int(time.time()), "data": data
                }
                self._write_device_map()
        elif kind == "flood_scope":
            scope = (item[1] or "").strip()
            if scope != self._default_flood_scope:
                self._default_flood_scope = scope
                Domoticz.Debug(f"Default flood scope: {scope or '(none)'}")
                self._write_device_map()
        elif kind == "device_info":
            info = dict(item[1] or {})
            # Firmware reports path_hash_mode in wire format (0/1/2 = 1/2/3
            # byte). The rest of our code and the dashboard work in the
            # human-friendly 1/2/3 form, so translate at the boundary.
            if "path_hash_mode" in info and isinstance(info["path_hash_mode"], int):
                info["path_hash_mode"] = info["path_hash_mode"] + 1
            if info != self._device_info:
                self._device_info = info
                fw = info.get("fw ver", "?")
                build = info.get("fw_build", "")
                model = info.get("model", "")
                Domoticz.Log(f"Device info: fw={fw} build={build!r} model={model!r}")
                self._write_device_map()
        elif kind == "self_info":
            name = item[1].get("name", "")
            Domoticz.Debug(f"Self info: name={name}, freq={item[1].get('radio_freq')} MHz")
            if name and name != self._self_name:
                self._self_name = name
            # Keep a full snapshot for the dashboard's self-node side panel.
            self._self_info_full = dict(item[1])
            self._write_device_map()
            # Track device-side settings so the dashboard can show / toggle them.
            # Skip while in the user-set grace window — some firmware returns
            # the previous value briefly while persisting to flash.
            in_grace = (time.monotonic() - self._settings_set_at) < SETTINGS_GRACE_S
            if not in_grace:
                mac = item[1].get("manual_add_contacts")
                if mac is not None:
                    self._manual_add_contacts = bool(mac)
                for k_attr, k_info in (
                    ("_telemetry_mode_base", "telemetry_mode_base"),
                    ("_telemetry_mode_loc",  "telemetry_mode_loc"),
                    ("_telemetry_mode_env",  "telemetry_mode_env"),
                    ("_advert_loc_policy",   "adv_loc_policy"),
                ):
                    v = item[1].get(k_info)
                    if v is not None:
                        setattr(self, k_attr, int(v))
            else:
                Domoticz.Debug(f"In settings grace window ({int(SETTINGS_GRACE_S - (time.monotonic() - self._settings_set_at))}s left); skipping self_info settings update.")
        elif kind == "send_result":
            d = item[1]
            # Internal control commands (!remove, !manual_add, !flood_scope,
            # !favorite, !set, ...) shouldn't appear in the inbox or count as
            # a sent mesh message.
            is_internal = isinstance(d.get("target"), str) and d["target"].startswith("!")
            if d["ok"]:
                if is_internal:
                    # The verb handler already logged its own success message;
                    # don't duplicate. Debug-level keeps it greppable.
                    Domoticz.Debug(f"Internal command ok: {d['target']} {d['body']}")
                    return
                Domoticz.Log(f"Message sent to '{d['target']}': {d['body']}")
                self._sent_count += 1
                if UNIT_MSGS_SENT_ in Devices:
                    Devices[UNIT_MSGS_SENT_].Update(nValue=0, sValue=str(self._sent_count))
                # Show sent message in the inbox so the user gets confirmation.
                # Use the same [ChannelName|sender] / [P|sender] format as incoming msgs
                # with a leading "> " on the sender to mark it as outgoing. Stick to
                # ASCII so Windows/cp1252 stored text isn't mangled.
                if UNIT_INBOX in Devices:
                    tgt = d["target"]
                    me = self._self_name or "Me"
                    if tgt.startswith("#"):
                        chan_idx_str = tgt[1:]
                        chan_idx_int = int(chan_idx_str) if chan_idx_str.isdigit() else None
                        chan_tag = self._channel_names.get(chan_idx_int, f"C{chan_idx_str}") if chan_idx_int is not None else f"C{chan_idx_str}"
                        Devices[UNIT_INBOX].Update(
                            nValue=0,
                            sValue=f"[{chan_tag}|> {me}] {d['body']}"
                        )
                    else:
                        Devices[UNIT_INBOX].Update(
                            nValue=0,
                            sValue=f"[P|> {tgt}] {d['body']}"
                        )
            else:
                Domoticz.Error(f"Send failed to '{d['target']}': {d['result']}")

    # ── Data handlers ─────────────────────────────────────────────────────────

    def _handle_contacts(self, contacts: dict):
        now = time.time()

        # Rebuild prefix → friendly-name lookup
        self._prefix_to_name = {
            c.get("public_key", "")[:12]: c.get("adv_name", "").strip()
            for c in contacts.values()
        }

        # Register any new contacts (non-self) in discovery order
        for contact in contacts.values():
            name = contact.get("adv_name", "").strip()
            if name and name != self._self_name and name not in self._contact_names:
                self._contact_names.append(name)
                Domoticz.Log(f"New contact discovered: '{name}'")

        # Update self node status from self_info (always online when connected)
        if self._self_name:
            self._ensure_node_devices(self._self_name)
            idx = self._node_index(self._self_name)
            if idx >= 0:
                status_unit = self._node_unit(idx, OFF_STATUS)
                if status_unit in Devices:
                    Devices[status_unit].Update(nValue=1, sValue="On")

        # Update all remote contacts
        for contact in contacts.values():
            node_name = contact.get("adv_name", "").strip()
            if not node_name or node_name == self._self_name:
                continue

            last_advert = contact.get("last_advert", 0)
            if last_advert < 1_577_836_800:
                last_advert = 0
            last_activity = self._node_last_activity.get(node_name, 0)
            effective_ts  = max(last_advert, last_activity)
            # Mirror the effective_ts into _node_last_activity so the device
            # map's last_seen field (which now reads from this dict) reflects
            # advert-based activity even when no messages have arrived.
            if effective_ts > last_activity:
                self._node_last_activity[node_name] = effective_ts
            advert_online = effective_ts > 0 and (now - effective_ts) < ONLINE_THRESHOLD_S

            path_len    = contact.get("out_path_len", -1)
            path_online = path_len >= 0
            online      = advert_online or path_online
            age_s       = int(now - effective_ts) if effective_ts > 0 else -1

            self._ensure_node_devices(node_name)
            idx = self._node_index(node_name)
            if idx < 0:
                continue

            status_unit = self._node_unit(idx, OFF_STATUS)
            if status_unit in Devices:
                Devices[status_unit].Update(
                    nValue=1 if online else 0,
                    sValue="On" if online else "Off"
                )

            hops_unit = self._node_unit(idx, OFF_HOPS)
            if hops_unit in Devices and path_len >= 0:
                Devices[hops_unit].Update(nValue=0, sValue=str(path_len))

            if effective_ts > 0:
                ls_unit = self._node_unit(idx, OFF_LASTSEEN)
                if ls_unit in Devices:
                    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(effective_ts))
                    Devices[ls_unit].Update(nValue=0, sValue=ts)

            la = self._node_last_activity.get(node_name, 0)

            # Store contact metadata for the dashboard map
            self._node_types[node_name] = int(contact.get("type", 0))
            if last_advert > 0:
                self._node_last_advert[node_name] = last_advert
            pk = contact.get("public_key", "")
            if pk:
                self._node_pubkey[node_name] = pk

            # Store GPS location if the contact advertises valid coordinates
            adv_lat = contact.get("adv_lat", 0.0)
            adv_lon = contact.get("adv_lon", 0.0)
            if adv_lat and adv_lon and not (adv_lat == 0.0 and adv_lon == 0.0):
                self._node_locations[node_name] = {"lat": adv_lat, "lon": adv_lon}

            Domoticz.Debug(
                f"Contact '{node_name}' type={contact.get('type',-1)}: "
                f"last_advert={int(now-last_advert)}s ago  "
                f"last_activity={int(now-la) if la else 'never'}  "
                f"path_len={path_len}  "
                f"online={online} (advert={advert_online} path={path_online})"
            )

        self._write_device_map()

    def _handle_message(self, msg: dict):
        """Handle an incoming message — update Inbox and per-node RSSI/SNR/LastSeen."""
        msg_type  = msg.get("type", "")
        text      = msg.get("text", "")

        # Resolve sender name
        prefix    = msg.get("pubkey_prefix", "")
        node_name = self._prefix_to_name.get(prefix, "").strip() if prefix else ""
        if prefix and not node_name:
            Domoticz.Debug(
                f"Incoming message: pubkey_prefix={prefix!r} did not match any known "
                f"contact (have {len(self._prefix_to_name)} prefixes). per-node "
                f"updates will be skipped."
            )

        # For CHAN messages the sender name is embedded in the text as "Name: text"
        # and there is no pubkey — use text prefix up to the first ": " as hint
        if not node_name and msg_type in ("CHAN", "channel_message"):
            if ": " in text:
                node_name = text.split(": ", 1)[0].strip()
                text_body = text.split(": ", 1)[1].strip()
            else:
                text_body = text
        else:
            text_body = text

        display_name = node_name or prefix or "?"

        # Channel tag: resolve index to name when available, fall back to C<idx>
        channel_idx = msg.get("channel_idx")
        if msg_type in ("CHAN", "channel_message") and channel_idx is not None:
            chan_tag = self._channel_names.get(channel_idx, f"C{channel_idx}")
        else:
            chan_tag = "P"

        # Update global inbox — format: [ChannelName|sender] text  or  [P|sender] text
        if UNIT_INBOX in Devices:
            Devices[UNIT_INBOX].Update(nValue=0, sValue=f"[{chan_tag}|{display_name}] {text_body}")

        # Update per-node devices for any known contact
        if node_name:
            self._ensure_node_devices(node_name)
            idx = self._node_index(node_name)
            if idx >= 0:
                now_ts = int(time.time())
                # Record activity — used by _handle_contacts for online detection
                self._node_last_activity[node_name] = now_ts

                # A message means the node is clearly reachable → mark online
                status_unit = self._node_unit(idx, OFF_STATUS)
                if status_unit in Devices:
                    Devices[status_unit].Update(nValue=1, sValue="On")

                # Last Seen
                ls_unit = self._node_unit(idx, OFF_LASTSEEN)
                if ls_unit in Devices:
                    Devices[ls_unit].Update(nValue=0, sValue=time.strftime("%Y-%m-%d %H:%M:%S"))

                # SNR from message metadata; RSSI from status poll (req_status_sync)
                snr = msg.get("SNR") if msg.get("SNR") is not None else msg.get("snr")
                if snr is not None:
                    snr_unit = self._node_unit(idx, OFF_SNR)
                    if snr_unit in Devices:
                        Devices[snr_unit].Update(nValue=0, sValue=str(round(float(snr), 2)))

                # Per-contact signal history for the dashboard sparkline.
                # CONTACT_MSG_RECV carries the sender's pubkey_prefix directly.
                pk_prefix = msg.get("pubkey_prefix", "")
                if pk_prefix and snr is not None:
                    with self._rx_log_lock:
                        hist = self._signal_history.setdefault(pk_prefix, [])
                        hist.append({"t": time.time(), "snr": float(snr), "rssi": None,
                                     "path_len": msg.get("path_len", -1), "kind": "MSG"})
                        if len(hist) > 60:
                            del hist[: len(hist) - 60]
                    self._rx_log_dirty = True

        self._write_device_map()

        # Increment message received counter
        self._recv_count += 1
        if UNIT_MSGS_RECV in Devices:
            Devices[UNIT_MSGS_RECV].Update(nValue=0, sValue=str(self._recv_count))

    def _handle_self_stats(self, stats: dict):
        """Update devices for the connected (self) node from polled stats."""
        if not self._self_name:
            return
        self._ensure_node_devices(self._self_name)
        idx = self._node_index(self._self_name)
        if idx < 0:
            return

        # Battery (millivolts) — from stats_core
        bat_mv = stats.get("battery_mv", 0)
        if bat_mv:
            pct   = _bat_pct(bat_mv)
            v     = round(bat_mv / 1000, 2)
            u_pct = self._node_unit(idx, OFF_BATT_PCT)
            u_v   = self._node_unit(idx, OFF_BATT_V)
            if u_pct in Devices:
                Devices[u_pct].Update(nValue=pct, sValue=str(pct))
            if u_v in Devices:
                Devices[u_v].Update(nValue=0, sValue=str(v))

        # Uptime (seconds → minutes)
        uptime_s = stats.get("uptime_secs", 0)
        if uptime_s:
            u = self._node_unit(idx, OFF_UPTIME)
            if u in Devices:
                Devices[u].Update(nValue=0, sValue=str(round(uptime_s / 60, 1)))

        # Radio stats
        noise = stats.get("noise_floor")
        if noise is not None:
            u = self._node_unit(idx, OFF_NOISE)
            if u in Devices:
                Devices[u].Update(nValue=0, sValue=str(noise))

        rssi = stats.get("last_rssi")
        if rssi is not None:
            u = self._node_unit(idx, OFF_RSSI)
            if u in Devices:
                Devices[u].Update(nValue=0, sValue=str(rssi))

        snr = stats.get("last_snr")
        if snr is not None:
            u = self._node_unit(idx, OFF_SNR)
            if u in Devices:
                Devices[u].Update(nValue=0, sValue=str(round(snr, 2)))

        # TX air seconds
        tx_air = stats.get("tx_air_secs")
        if tx_air is not None:
            u = self._node_unit(idx, OFF_AIRTIME)
            if u in Devices:
                Devices[u].Update(nValue=0, sValue=str(tx_air))

        # Packet counters
        pkt_sent = stats.get("sent")
        if pkt_sent is not None:
            u = self._node_unit(idx, OFF_MSGS_SENT)
            if u in Devices:
                Devices[u].Update(nValue=0, sValue=str(pkt_sent))

        pkt_recv = stats.get("recv")
        if pkt_recv is not None:
            u = self._node_unit(idx, OFF_MSGS_RECV)
            if u in Devices:
                Devices[u].Update(nValue=0, sValue=str(pkt_recv))

        # Last seen = now (we just got data from it)
        ls_unit = self._node_unit(idx, OFF_LASTSEEN)
        if ls_unit in Devices:
            Devices[ls_unit].Update(nValue=0, sValue=time.strftime("%Y-%m-%d %H:%M:%S"))

        Domoticz.Debug(f"Self stats updated: bat={bat_mv}mV uptime={uptime_s}s rssi={rssi} snr={stats.get('last_snr')}")
        self._write_device_map()


# ── Domoticz plugin entry points ─────────────────────────────────────────────

_plugin = BasePlugin()

def onStart():            _plugin.onStart()
def onStop():             _plugin.onStop()
def onHeartbeat():        _plugin.onHeartbeat()
def onDeviceModified(u):  _plugin.onDeviceModified(u)
