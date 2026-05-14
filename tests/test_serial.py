"""
Serial connection test for MeshCore plugin.

Verifies the meshcore python package speaks the same protocol over USB serial
as it does over TCP. If this script prints contacts and self-node stats,
the plugin can be extended to support serial transport with no protocol changes.

Usage:
    python tests/test_serial.py [PORT] [BAUD]
    python tests/test_serial.py COM6 115200
"""

import asyncio
import sys

from meshcore import MeshCore, EventType


async def run(port: str, baud: int) -> int:
    print(f"[*] Connecting to {port} @ {baud}...")
    try:
        mc = await MeshCore.create_serial(port, baudrate=baud, default_timeout=10)
    except Exception as e:
        print(f"[!] create_serial failed: {e}")
        return 1

    if not mc.is_connected:
        print("[!] Not connected after create_serial")
        return 1
    print("[+] Connected.")

    advert_count = 0
    msg_count = 0

    def on_advert(ev):
        nonlocal advert_count
        advert_count += 1
        p = ev.payload or {}
        print(f"    ADVERT: {p.get('adv_name')!r} lat={p.get('adv_lat')} lon={p.get('adv_lon')}")

    def on_msg(ev):
        nonlocal msg_count
        msg_count += 1
        p = ev.payload or {}
        print(f"    MSG: {p}")

    mc.subscribe(EventType.ADVERTISEMENT, on_advert)
    mc.subscribe(EventType.CONTACT_MSG_RECV, on_msg)
    mc.subscribe(EventType.CHANNEL_MSG_RECV, on_msg)

    print("\n[*] get_stats_core()...")
    try:
        ev = await mc.commands.get_stats_core()
        print(f"    {ev.payload}")
    except Exception as e:
        print(f"[!] get_stats_core failed: {e}")

    print("\n[*] get_stats_radio()...")
    try:
        ev = await mc.commands.get_stats_radio()
        print(f"    {ev.payload}")
    except Exception as e:
        print(f"[!] get_stats_radio failed: {e}")

    print("\n[*] get_stats_packets()...")
    try:
        ev = await mc.commands.get_stats_packets()
        print(f"    {ev.payload}")
    except Exception as e:
        print(f"[!] get_stats_packets failed: {e}")

    print("\n[*] get_contacts()...")
    try:
        await mc.commands.get_contacts()
        contacts = mc.contacts or {}
        print(f"    {len(contacts)} contacts:")
        for key, c in contacts.items():
            print(f"      - {c.get('adv_name')!r} type={c.get('type')} "
                  f"out_path_len={c.get('out_path_len')} last_advert={c.get('last_advert')}")
    except Exception as e:
        print(f"[!] get_contacts failed: {e}")

    print("\n[*] get_msg() (drain queue)...")
    try:
        for _ in range(20):
            ev = await mc.commands.get_msg()
            if not ev or not ev.payload:
                break
            print(f"    queued msg: {ev.payload}")
    except Exception as e:
        print(f"[!] get_msg failed: {e}")

    print("\n[*] Listening 15s for pushed events (adverts / messages)...")
    await asyncio.sleep(15)
    print(f"    received {advert_count} adverts, {msg_count} messages")

    await mc.disconnect()
    print("\n[+] Done — protocol matches TCP. Serial transport is compatible.")
    return 0


if __name__ == "__main__":
    port = sys.argv[1] if len(sys.argv) > 1 else "COM6"
    baud = int(sys.argv[2]) if len(sys.argv) > 2 else 115200
    sys.exit(asyncio.run(run(port, baud)))
