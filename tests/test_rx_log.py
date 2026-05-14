"""Probe whether the connected device emits RX_LOG_DATA events.

Connects via serial, subscribes to push events for raw RX log data, listens for
~30 seconds, and reports what (if anything) arrives. If the firmware emits
LOG_DATA packets, we'll see full per-frame metadata: header byte, route_type,
payload_type, transport_code (region/scope), path, ciphertext, etc.

Run while Domoticz / the plugin is NOT actively talking to the same port
(the plugin's short-lived connections will release the port between heartbeats,
so you can usually slip in).

Usage:
    python tests/test_rx_log.py [PORT] [SECONDS]
    python tests/test_rx_log.py COM6 30
"""

import asyncio
import sys
import time

from meshcore import MeshCore, EventType


async def run(port: str, seconds: int):
    print(f"[*] Connecting to {port}...")
    try:
        mc = await MeshCore.create_serial(port, baudrate=115200, default_timeout=10)
    except Exception as e:
        print(f"[!] connect failed: {e}")
        return
    if not mc.is_connected:
        print("[!] not connected after create")
        return
    print("[+] connected")

    rx_log_count = 0
    raw_data_count = 0
    log_data_count = 0
    trace_data_count = 0

    def on_rx_log(ev):
        nonlocal rx_log_count
        rx_log_count += 1
        p = ev.payload or {}
        # Print key analyzer fields
        print(f"\n=== RX_LOG_DATA #{rx_log_count} ===")
        for k in [
            "snr", "rssi", "recv_time",
            "header", "route_type", "route_typename",
            "payload_type", "payload_typename", "payload_ver",
            "transport_code",
            "path_len", "path_hash_size", "path",
            "chan_hash", "cipher_mac",
            "message",
            "raw_hex", "payload",
        ]:
            if k in p:
                v = p[k]
                # Trim long hex strings for readability
                if isinstance(v, str) and len(v) > 80:
                    v = v[:80] + f"... (+{len(v)-80} more chars)"
                print(f"  {k:20s} = {v!r}")

    def on_raw(ev):
        nonlocal raw_data_count
        raw_data_count += 1
        print(f"\n--- RAW_DATA #{raw_data_count}: {ev.payload!r}")

    def on_log(ev):
        nonlocal log_data_count
        log_data_count += 1
        print(f"\n--- LOG_DATA #{log_data_count}: {ev.payload!r}")

    def on_trace(ev):
        nonlocal trace_data_count
        trace_data_count += 1
        print(f"\n--- TRACE_DATA #{trace_data_count}: {ev.payload!r}")

    mc.subscribe(EventType.RX_LOG_DATA, on_rx_log)
    mc.subscribe(EventType.RAW_DATA,    on_raw)
    mc.subscribe(EventType.LOG_DATA,    on_log)
    mc.subscribe(EventType.TRACE_DATA,  on_trace)

    print(f"[*] Listening for {seconds}s for RX_LOG_DATA / RAW_DATA / LOG_DATA / TRACE_DATA push events...")
    print("    (send/receive some mesh traffic to provoke events)")
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        await asyncio.sleep(0.5)

    print("\n" + "=" * 60)
    print(f"Summary after {seconds}s:")
    print(f"  RX_LOG_DATA : {rx_log_count}")
    print(f"  RAW_DATA    : {raw_data_count}")
    print(f"  LOG_DATA    : {log_data_count}")
    print(f"  TRACE_DATA  : {trace_data_count}")
    print("=" * 60)
    if rx_log_count == 0 and log_data_count == 0:
        print("\n[!] No RX log events seen. The firmware on this device probably does")
        print("    NOT emit LOG_DATA packets. Implementing analyzer-style detail would")
        print("    require either a firmware update or an alternative path.")
    else:
        print("\n[+] Firmware emits RX log events — analyzer-style detail is implementable.")

    await mc.disconnect()


if __name__ == "__main__":
    port = sys.argv[1] if len(sys.argv) > 1 else "COM6"
    seconds = int(sys.argv[2]) if len(sys.argv) > 2 else 30
    asyncio.run(run(port, seconds))
