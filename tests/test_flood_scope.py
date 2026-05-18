"""Quick test: read and set default flood scope over serial."""
import asyncio, sys
from meshcore import MeshCore, EventType


async def run(port: str, scope: str | None):
    print(f"[*] Connecting to {port}...")
    mc = await MeshCore.create_serial(port, baudrate=115200, default_timeout=10)
    if not mc.is_connected:
        print("[!] Not connected")
        return

    print("[*] get_default_flood_scope (before)...")
    r = await mc.commands.get_default_flood_scope()
    print(f"    type={r.type}")
    print(f"    payload={r.payload}")

    if scope is not None:
        print(f"\n[*] set_default_flood_scope({scope!r})...")
        r = await mc.commands.set_default_flood_scope(scope or None)
        print(f"    type={r.type}  payload={r.payload}")

        print("\n[*] get_default_flood_scope (after)...")
        r = await mc.commands.get_default_flood_scope()
        print(f"    type={r.type}")
        print(f"    payload={r.payload}")

    await mc.disconnect()


if __name__ == "__main__":
    port = sys.argv[1] if len(sys.argv) > 1 else "COM6"
    scope = sys.argv[2] if len(sys.argv) > 2 else None
    asyncio.run(run(port, scope))
