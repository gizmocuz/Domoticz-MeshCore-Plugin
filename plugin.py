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

# How many heartbeats between self-node stats polls (heartbeat=30s, so 10 = ~5 min)
SELF_STATS_HEARTBEATS = 10

# Connection timeout for each short-lived TCP session (seconds)
CONNECT_TIMEOUT    = 12
COMMAND_TIMEOUT    = 10
WORKER_TIMEOUT     = 60    # max seconds a worker thread may run before being abandoned
POST_DISCONNECT_S  = 3     # minimum seconds to wait after disconnect before reconnecting

# Backoff on consecutive connection failures
BACKOFF_BASE_S     = 60     # first extra delay on failure (added on top of the heartbeat interval)
BACKOFF_MAX_S      = 600    # 10 min max extra delay
BACKOFF_FACTOR     = 2.0
BACKOFF_LOG_THRESH = 3      # after this many consecutive failures, downgrade Error→Debug

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
        # Fetched once per successful poll cycle and mirrored to the device map.
        self._device_info: dict = {}
        # Have we bumped heartbeat back to 30s after the initial fast drain?
        self._heartbeat_restored: bool = False
        # Last sValue we already dispatched — prevents re-sending on every heartbeat
        self._last_sent_text = ""
        # Message counters (reset when Domoticz restarts the plugin)
        self._recv_count = 0
        self._sent_count = 0
        # Heartbeat counter for periodic self-stats poll
        self._hb_count = 0
        # Backoff state for consecutive connection failures
        self._consec_failures = 0
        self._skip_until = 0.0  # monotonic time: skip heartbeats until this
        # Channel names already fetched flag (only need once)
        self._channels_fetched = False
        # Channel index→name map (populated from device), e.g. {0: "General", 1: "MyRoom"}
        self._channel_names: dict = {}
        # Lock to serialise all TCP connections (ESP32 accepts only one at a time)
        self._conn_lock = threading.Lock()
        # Current mc instance (set by worker thread for cleanup on error)
        self._current_mc = None
        # Active worker thread reference (for onStop cleanup)
        self._worker_thread: threading.Thread | None = None
        self._worker_started: float = 0.0
        # Flag to prevent new connections during shutdown
        self._stopping = False
        # Monotonic timestamp of last successful disconnect (for cooldown)
        self._last_disconnect: float = 0.0

    def _force_close_serial(self, mc):
        """Force-close the underlying pyserial port synchronously.

        meshcore's SerialConnection.disconnect() only schedules transport.close();
        the actual OS-level port release happens later in connection_lost.
        On Windows this means COM ports linger and can't be reopened for several
        seconds. Grab the raw pyserial Serial object and close it ourselves.
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
            try:
                connection.transport = None
            except Exception:
                pass
        except Exception:
            pass

    def _force_kill_socket(self, mc):
        """Kill the raw TCP socket without touching the event loop."""
        import socket as _socket
        import struct
        try:
            transport = mc.connection_manager.connection.transport
            if transport:
                raw_sock = transport.get_extra_info("socket")
                if raw_sock:
                    try:
                        raw_sock.setsockopt(
                            _socket.SOL_SOCKET, _socket.SO_LINGER,
                            struct.pack("ii", 1, 0)
                        )
                    except OSError:
                        pass
                transport.close()
                if raw_sock:
                    try:
                        raw_sock.close()
                    except OSError:
                        pass
        except Exception:
            pass
        try:
            if mc.dispatcher and mc.dispatcher._task and not mc.dispatcher._task.done():
                mc.dispatcher.running = False
                mc.dispatcher._task.cancel()
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

    def _safe_disconnect(self, mc, loop):
        """Cleanly disconnect using the library's own mc.disconnect(), then
        fall back to a hard close if that fails or times out."""
        if mc is None:
            return
        # Try the library's graceful disconnect first
        try:
            loop.run_until_complete(asyncio.wait_for(mc.disconnect(), timeout=5))
            Domoticz.Debug("_safe_disconnect: graceful disconnect OK.")
        except Exception:
            Domoticz.Debug("_safe_disconnect: graceful failed, force-closing...")
            if self.transport == "Serial":
                self._force_close_serial(mc)
            else:
                self._force_kill_socket(mc)
        # For serial: ALWAYS force-close the raw pyserial port too. mc.disconnect()
        # only schedules transport.close() — the OS handle isn't released until
        # the loop processes connection_lost, which won't happen if we close
        # the loop next. On Windows this would prevent the next open() for
        # several seconds.
        if self.transport == "Serial":
            self._force_close_serial(mc)
            # Pump the loop briefly so connection_lost fires while we still own it
            try:
                loop.run_until_complete(asyncio.sleep(0.1))
            except Exception:
                pass
        # Cancel any remaining asyncio tasks (don't await — just cancel)
        try:
            for task in asyncio.all_tasks(loop):
                task.cancel()
        except Exception:
            pass
        time.sleep(0.3 if self.transport == "Serial" else 0.5)
        self._last_disconnect = time.monotonic()
        Domoticz.Debug("_safe_disconnect: done.")

    async def _async_disconnect(self, mc):
        """Graceful disconnect for use inside async poll/send cycles."""
        if mc is None:
            return
        try:
            await asyncio.wait_for(mc.disconnect(), timeout=5)
            Domoticz.Debug("_async_disconnect: OK.")
        except Exception:
            Domoticz.Debug("_async_disconnect: graceful failed, force-closing...")
            if self.transport == "Serial":
                self._force_close_serial(mc)
            else:
                self._force_kill_socket(mc)
        # For serial: force-close the raw pyserial port and let the loop
        # process connection_lost so Windows actually releases the COM handle.
        if self.transport == "Serial":
            self._force_close_serial(mc)
            try:
                await asyncio.sleep(0.1)
            except Exception:
                pass
        self._last_disconnect = time.monotonic()

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
        # Start with a fast heartbeat so the queued results from the immediate
        # connect (below) are drained right away. _dispatch() bumps this back
        # to 30s as soon as the first contacts batch is processed.
        Domoticz.Heartbeat(2)
        if self.transport == "Serial":
            Domoticz.Log(f"MeshCore plugin started - Serial {self.serial_port} @ {self.baud_rate}")
        else:
            Domoticz.Log(f"MeshCore plugin started - TCP {self.host}:{self.port}")

        # Trigger an immediate connect so the user doesn't wait 30s for the
        # first heartbeat tick.
        if self._conn_lock.acquire(blocking=False):
            self._hb_count += 1
            t = threading.Thread(target=self._heartbeat_worker, daemon=True, name="MeshCorePoll")
            self._worker_thread = t
            self._worker_started = time.monotonic()
            t.start()

    def onStop(self):
        self._stopping = True
        self.initialized = False

        # Forcefully close any active connection so the device/port is released.
        # For TCP this RST-closes the socket (ESP32 needs that); for serial this
        # closes the underlying pyserial Serial object synchronously so Windows
        # releases the COM handle immediately.
        mc = self._current_mc
        if mc is not None:
            Domoticz.Log("onStop: force-closing active connection...")
            if self.transport == "Serial":
                self._force_close_serial(mc)
            else:
                self._force_kill_socket(mc)
            Domoticz.Log("onStop: connection closed.")

        # Wait for any running worker thread to finish
        t = self._worker_thread
        if t is not None and t.is_alive():
            t.join(timeout=5)
            if t.is_alive():
                Domoticz.Log("onStop: worker thread did not stop within 5s.")

        self._remove_custom_page()
        Domoticz.Log("MeshCore plugin stopped.")

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

        # Backoff: skip this heartbeat if we are in a cooldown period
        now = time.monotonic()
        if now < self._skip_until:
            remaining = int(self._skip_until - now)
            Domoticz.Debug(f"Backoff active — skipping heartbeat ({remaining}s remaining)")
            return

        # Watchdog: if a worker thread has been running longer than WORKER_TIMEOUT,
        # force-kill its connection and release the lock.
        t = self._worker_thread
        if t is not None and t.is_alive():
            elapsed = time.monotonic() - self._worker_started
            if elapsed > WORKER_TIMEOUT:
                Domoticz.Error(f"Watchdog: worker thread hung for {int(elapsed)}s — force-killing connection")
                mc = self._current_mc
                if mc is not None:
                    self._force_kill_socket(mc)
                # Give the thread a moment to die after socket kill
                t.join(timeout=2)
                if not t.is_alive():
                    Domoticz.Log("Watchdog: worker thread exited after socket kill.")
                else:
                    Domoticz.Error("Watchdog: worker thread still alive — force-releasing lock.")
                    # Force-release the lock so we can continue
                    try:
                        self._conn_lock.release()
                    except RuntimeError:
                        pass
                self._worker_thread = None

        # Cooldown: don't reconnect too soon after the last disconnect
        if self._last_disconnect > 0:
            since = time.monotonic() - self._last_disconnect
            if since < POST_DISCONNECT_S:
                Domoticz.Debug(f"Post-disconnect cooldown ({POST_DISCONNECT_S - since:.0f}s remaining)")
                return

        # Prevent overlapping connections (previous heartbeat or send still running)
        if not self._conn_lock.acquire(blocking=False):
            Domoticz.Debug("Previous connection still active — skipping heartbeat")
            return

        self._hb_count += 1
        t = threading.Thread(target=self._heartbeat_worker, daemon=True, name="MeshCorePoll")
        self._worker_thread = t
        self._worker_started = time.monotonic()
        t.start()

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
        # !favorite only touches plugin-side state + the favorites JSON file, so
        # spawning a full connect/disconnect worker would be pointless 1-3s lag.
        if self._handle_local_only_command(text):
            return

        Domoticz.Log(f"Sending message immediately: {text}")
        t = threading.Thread(
            target=self._immediate_send_worker, args=(text,),
            daemon=True, name="MeshCoreSend"
        )
        self._worker_thread = t
        self._worker_started = time.monotonic()
        t.start()

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
            nodes[node_name] = {
                "status":    _slot(self._node_unit(ni, OFF_STATUS)),
                "battery":   _slot(self._node_unit(ni, OFF_BATT_PCT)),
                "battery_v": _slot(self._node_unit(ni, OFF_BATT_V)),
                "rssi":      _slot(self._node_unit(ni, OFF_RSSI)),
                "snr":       _slot(self._node_unit(ni, OFF_SNR)),
                "noise":     _slot(self._node_unit(ni, OFF_NOISE)),
                "last_seen": _slot(self._node_unit(ni, OFF_LASTSEEN)),
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

    def _remove_custom_page(self):
        plugin_dir    = os.path.dirname(os.path.abspath(__file__))
        domoticz_root = os.path.abspath(os.path.join(plugin_dir, "..", ".."))
        fname = "meshcore.html"
        dest = os.path.join(domoticz_root, "www", "templates", fname)
        try:
            if os.path.isfile(dest):
                os.remove(dest)
        except Exception as exc:
            Domoticz.Error(f"Failed to remove {fname}: {exc}")
        Domoticz.Log("MeshCore dashboard removed.")

    # ── Heartbeat-driven poll worker ─────────────────────────────────────────

    def _heartbeat_worker(self):
        """Run a short-lived connect→poll→disconnect cycle in a background thread."""
        Domoticz.Debug("Worker: started")
        loop = asyncio.new_event_loop()
        self._current_mc = None
        try:
            Domoticz.Debug("Worker: entering poll cycle...")
            loop.run_until_complete(self._poll_cycle(loop))
            Domoticz.Debug("Worker: poll cycle completed OK")
        except Exception as exc:
            self._consec_failures = min(self._consec_failures + 1, 20)  # cap counter
            if self._consec_failures <= BACKOFF_LOG_THRESH:
                Domoticz.Debug(f"Worker: poll error: {exc}")
            else:
                Domoticz.Debug(f"Worker: poll error (failure #{self._consec_failures}): {exc}")
            # Serial: rely on the 30s heartbeat for reconnect — no extra backoff.
            # TCP: progressive backoff so the ESP32 can recover.
            if self.transport != "Serial":
                delay = min(BACKOFF_BASE_S * (BACKOFF_FACTOR ** min(self._consec_failures, 10)), BACKOFF_MAX_S)
                if delay > 0:
                    self._skip_until = time.monotonic() + delay
                    Domoticz.Log(f"Worker: backing off {int(delay)}s (failure #{self._consec_failures})")
        finally:
            Domoticz.Debug("Worker: entering finally block...")
            if self._current_mc is not None:
                Domoticz.Debug("Worker: calling _safe_disconnect...")
                self._safe_disconnect(self._current_mc, loop)
                self._current_mc = None
            Domoticz.Debug("Worker: closing event loop...")
            try:
                loop.close()
            except Exception:
                pass
            Domoticz.Debug("Worker: releasing _conn_lock...")
            self._conn_lock.release()
            Domoticz.Debug("Worker: done.")

    def _immediate_send_worker(self, text: str):
        """Short-lived connect → send → disconnect fired from onDeviceModified."""
        Domoticz.Debug(f"SendWorker: waiting for _conn_lock...")
        if not self._conn_lock.acquire(timeout=30):
            Domoticz.Error("SendWorker: _conn_lock timeout after 30s")
            self._queue.put(("send_result", {"ok": False, "target": "?",
                                              "body": text, "result": "connection busy (timeout)"}))
            return

        Domoticz.Debug(f"SendWorker: lock acquired, starting send cycle...")
        loop = asyncio.new_event_loop()
        self._current_mc = None
        try:
            loop.run_until_complete(self._send_cycle(text, loop))
            Domoticz.Debug("SendWorker: send cycle completed OK")
        except Exception as exc:
            Domoticz.Error(f"SendWorker: error: {exc}")
            self._queue.put(("send_result", {"ok": False, "target": "?",
                                              "body": text, "result": str(exc)}))
        finally:
            Domoticz.Debug("SendWorker: entering finally block...")
            if self._current_mc is not None:
                Domoticz.Debug("SendWorker: calling _safe_disconnect...")
                self._safe_disconnect(self._current_mc, loop)
                self._current_mc = None
            Domoticz.Debug("SendWorker: closing event loop...")
            try:
                loop.close()
            except Exception:
                pass
            Domoticz.Debug("SendWorker: releasing _conn_lock...")
            self._conn_lock.release()
            Domoticz.Debug("SendWorker: done.")

    async def _connect_with_retry(self, label: str, max_attempts: int = 3):
        """Connect to the device with retries on appstart failure.

        Uses MeshCore.create_tcp() or MeshCore.create_serial() based on
        self.transport. On success emits a "Connected" log if we had previously
        been disconnected; on failure emits an "Error" log on the first failure
        only (subsequent failures while still down are debug-level).
        Returns the connected mc instance or raises ConnectionError.
        """
        # Serial retries are simpler — the next heartbeat (30s) will retry again
        # anyway. TCP needs short progressive delays so the ESP32 can release
        # its single TCP slot.
        if self.transport == "Serial":
            retry_delays = [2, 4]
            max_attempts = min(max_attempts, 2)
        else:
            retry_delays = [3, 6, 10]

        last_err = None
        for attempt in range(1, max_attempts + 1):
            Domoticz.Debug(f"{label}: connect attempt {attempt}/{max_attempts}...")

            mc = None
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
                Domoticz.Debug(f"{label}: connect exception on attempt {attempt}: {exc}")
                if mc is not None:
                    self._current_mc = mc
                    await self._async_disconnect(mc)
                last_err = exc
                if attempt < max_attempts:
                    delay = retry_delays[min(attempt - 1, len(retry_delays) - 1)]
                    await asyncio.sleep(delay)
                continue

            self._current_mc = mc
            if mc is not None and mc.is_connected:
                if not self._was_connected:
                    Domoticz.Log(f"{label}: Connected to MeshCore device.")
                else:
                    Domoticz.Debug(f"{label}: connected on attempt {attempt}.")
                self._was_connected = True
                return mc

            Domoticz.Debug(f"{label}: not connected after create, retrying...")
            await self._async_disconnect(mc)
            last_err = ConnectionError("Device did not connect.")
            if attempt < max_attempts:
                delay = retry_delays[min(attempt - 1, len(retry_delays) - 1)]
                await asyncio.sleep(delay)

        # All attempts failed
        endpoint = (f"serial {self.serial_port} @ {self.baud_rate}"
                    if self.transport == "Serial"
                    else f"{self.host}:{self.port}")
        if self._was_connected:
            Domoticz.Error(
                f"{label}: Lost connection to MeshCore device ({endpoint}). "
                f"Will retry every 30s. Error: {last_err}"
            )
            self._was_connected = False
        else:
            # First-time connect or still-not-yet-connected: log once as Error
            # so the user sees the reason, then log a heartbeat every 5 minutes
            # so it's obvious the plugin is still trying.
            if self._consec_failures == 0:
                Domoticz.Error(f"{label}: Could not connect to MeshCore device ({endpoint}): {last_err}. Will retry every 30s.")
            elif (self._consec_failures + 1) % 10 == 0:
                # ~5 minutes between visible retry messages (10 heartbeats × 30s)
                Domoticz.Log(f"{label}: Still trying to reconnect to {endpoint} (attempt #{self._consec_failures + 1}): {last_err}")
            else:
                Domoticz.Debug(f"{label}: still not connected (attempt #{self._consec_failures + 1}): {last_err}")
        raise last_err or ConnectionError("Failed to connect after retries.")

    async def _send_cycle(self, text: str, loop):
        """Connect, send one message, disconnect.  Stores mc on self._current_mc."""
        mc = await self._connect_with_retry("SendCycle")

        # We need contacts loaded for name → contact lookup
        if not mc.contacts:
            Domoticz.Debug("SendCycle: fetching contacts...")
            await asyncio.wait_for(mc.commands.get_contacts(), timeout=COMMAND_TIMEOUT)
            await asyncio.sleep(0)
            await asyncio.sleep(0)

        Domoticz.Debug(f"SendCycle: sending message...")
        await self._send_message(mc, text)
        Domoticz.Debug("SendCycle: message sent.")

        # Gracefully disconnect inside the async context (proper TCP FIN)
        await self._async_disconnect(mc)
        self._current_mc = None

    async def _poll_cycle(self, loop):
        """Connect, do all work.  Stores mc on self._current_mc for cleanup."""
        mc = await self._connect_with_retry("Poll")

        Domoticz.Debug(f"Poll: connected. self_info keys={list(mc.self_info.keys()) if mc.self_info else 'None'}")
        if mc.self_info:
            name = mc.self_info.get("name", "")
            if name:
                self._self_name = name
            self._queue.put(("self_info", dict(mc.self_info)))

        # ── Fetch contacts ────────────────────────────────────────────────
        Domoticz.Debug("Poll: fetching contacts...")
        for attempt in range(5):
            try:
                await asyncio.wait_for(mc.commands.get_contacts(), timeout=COMMAND_TIMEOUT)
            except asyncio.TimeoutError:
                Domoticz.Debug(f"get_contacts timed out (attempt {attempt + 1})")
            except Exception as exc:
                Domoticz.Debug(f"get_contacts error (attempt {attempt + 1}): {exc}")
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            if mc.contacts:
                break
            await asyncio.sleep(1)

        if mc.contacts:
            Domoticz.Debug(f"Poll: got {len(mc.contacts)} contact(s)")
            contacts_snapshot = {k: dict(v) for k, v in mc.contacts.items()}
            self._queue.put(("contacts", contacts_snapshot))
        else:
            Domoticz.Error("Poll: no contacts returned from device.")

        # ── Fetch channel names (once) ────────────────────────────────────
        if not self._channels_fetched:
            Domoticz.Log("Poll: fetching channel names...")
            await self._fetch_channel_names(mc)
            self._channels_fetched = True

        # ── Fetch default flood scope ─────────────────────────────────────
        try:
            r = await asyncio.wait_for(mc.commands.get_default_flood_scope(), timeout=5.0)
            Domoticz.Debug(f"get_default_flood_scope: type={r.type if r else None} payload={r.payload if r else None}")
            if r and r.type == EventType.DEFAULT_FLOOD_SCOPE:
                scope_name = (r.payload or {}).get("scope_name", "") or ""
                self._queue.put(("flood_scope", scope_name))
        except Exception as exc:
            Domoticz.Debug(f"get_default_flood_scope error: {exc}")

        # ── Fetch device info (firmware version / build / model) ──────────
        # Only refresh if we don't have it yet, or every SELF_STATS_HEARTBEATS cycle.
        if not self._device_info or (self._hb_count % SELF_STATS_HEARTBEATS == 0):
            try:
                r = await asyncio.wait_for(mc.commands.send_device_query(), timeout=5.0)
                if r and r.type == EventType.DEVICE_INFO:
                    self._queue.put(("device_info", dict(r.payload or {})))
            except Exception as exc:
                Domoticz.Debug(f"send_device_query error: {exc}")

        # ── Collect incoming messages ─────────────────────────────────────
        Domoticz.Debug("Poll: draining push events...")
        await self._drain_push_events(mc)

        # ── Poll self-node stats periodically ─────────────────────────────
        if self._hb_count % SELF_STATS_HEARTBEATS == 0:
            Domoticz.Debug("Poll: polling self stats...")
            await self._poll_self_stats(mc)

        # Connection succeeded — reset backoff
        self._consec_failures = 0
        self._skip_until = 0.0

        # Gracefully disconnect inside the async context (proper TCP FIN)
        await self._async_disconnect(mc)
        self._current_mc = None

        Domoticz.Debug("Poll: cycle complete.")

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
            Domoticz.Log(f"Fetched {fetched} pending message(s) from device.")

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
        """Query channel names for indices 0-7 and write meshcore_channels.json."""
        channel_names = {}
        for idx in range(8):
            try:
                res = await asyncio.wait_for(mc.commands.get_channel(idx), timeout=2.0)
                Domoticz.Debug(f"get_channel({idx}): type={res.type if res else None} payload={res.payload if res else None}")
                if res and res.type == EventType.CHANNEL_INFO:
                    name = res.payload.get("channel_name", "").strip("\x00").strip()
                    if name:
                        channel_names[str(idx)] = name
                elif res and res.type == EventType.ERROR:
                    # ERROR = no more channels, stop probing
                    break
            except asyncio.TimeoutError:
                # Unconfigured channels may simply not respond — skip, don't abort
                Domoticz.Debug(f"get_channel({idx}) timed out — skipping")
                continue
            except Exception as exc:
                Domoticz.Debug(f"get_channel({idx}) error: {exc} — skipping")
                continue
        if channel_names:
            self._channel_names = {int(k): v for k, v in channel_names.items()}
            parts = [f"#{k} = {v}" for k, v in sorted(channel_names.items())]
            Domoticz.Log(f"MeshCore channels: {', '.join(parts)}")
            self._write_channel_names(channel_names)
        else:
            Domoticz.Debug("No channel names found on device.")

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

        # ── Special: set default flood scope ───────────────────────────────
        # Syntax: "!flood_scope <name>"  (empty name = reset to global flood)
        if text.startswith("!flood_scope"):
            arg = text[len("!flood_scope"):].strip()
            # Library treats "", "0", "None", "*" as reset
            scope_to_set = arg if arg else None
            try:
                result = await asyncio.wait_for(
                    mc.commands.set_default_flood_scope(scope_to_set), timeout=10.0
                )
                ok = result is not None and result.type == EventType.OK
                if ok:
                    # Normalize the stored value to what the device would echo back
                    if scope_to_set is None:
                        self._default_flood_scope = ""
                    else:
                        s = scope_to_set
                        if not s.startswith("#"):
                            s = "#" + s
                        self._default_flood_scope = s
                    Domoticz.Log(f"Default flood scope set to {self._default_flood_scope or '(none)'}")
                    self._write_device_map()
                self._queue.put(("send_result", {
                    "ok": ok, "target": "!flood_scope",
                    "body": arg or "(reset)",
                    "result": "applied" if ok else str(result),
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
        elif kind == "contacts":
            self._handle_contacts(item[1])
            # First contacts batch processed — restore the normal 30 s
            # heartbeat. (We used a fast heartbeat at startup so the initial
            # queue drain was not delayed by 30 s.)
            if not self._heartbeat_restored:
                Domoticz.Heartbeat(30)
                self._heartbeat_restored = True
        elif kind == "self_stats":
            self._handle_self_stats(item[1])
        elif kind == "flood_scope":
            scope = (item[1] or "").strip()
            if scope != self._default_flood_scope:
                self._default_flood_scope = scope
                Domoticz.Debug(f"Default flood scope: {scope or '(none)'}")
                self._write_device_map()
        elif kind == "device_info":
            info = item[1] or {}
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
                Domoticz.Log(f"Message sent to '{d['target']}': {d['body']}")
                if is_internal:
                    return
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
