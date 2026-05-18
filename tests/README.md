# Tests

Manual / smoke-test scripts. They talk directly to a connected MeshCore device
via the [`meshcore`](https://pypi.org/project/meshcore/) Python package, so
**Domoticz does not need to be running** when you execute them. If it *is*
running and connected to the same port, stop the plugin first (or take advantage
of the plugin's short-lived connections — between heartbeats the COM port is
released and these scripts can connect for a few seconds).

## Requirements

```
pip install meshcore
```

## Scripts

### `test_serial.py`

Smoke-test serial connectivity. Connects via USB, dumps `get_stats_core`,
`get_stats_radio`, `get_stats_packets`, the contact list, then listens for 15 s
of push events (adverts + messages). Confirms that the meshcore protocol is
identical over serial and TCP.

```
python tests/test_serial.py [PORT] [BAUD]
python tests/test_serial.py COM6 115200
python tests/test_serial.py /dev/ttyUSB0
```

### `test_flood_scope.py`

Reads (and optionally writes) the device-side default flood scope. Use this to
verify the scope is persisting and that the plugin's `!flood_scope` command is
reaching the firmware.

```
python tests/test_flood_scope.py [PORT] [SCOPE]
python tests/test_flood_scope.py COM6               # read only
python tests/test_flood_scope.py COM6 "#nl"         # set #nl, then read back
python tests/test_flood_scope.py COM6 ""            # reset to global flood
```
