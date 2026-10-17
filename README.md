# Delta solar collector

Rust/Tokio collector for two Delta M4-TL-US inverters on a Raspberry Pi 5
running **64-bit Debian Trixie**. One rootful Podman Quadlet uses host BlueZ
over D-Bus and writes to the host's **InfluxDB 1.x**, database **solar**, without
authentication. The Dockerfile produces a standard OCI image usable by Docker
or Podman; Quadlet itself uses Podman.

## Install on the Pi

Run these commands from this project directory on the Pi:

```sh
sudo apt-get update
sudo apt-get install -y podman bluez curl
sudo systemctl enable --now bluetooth
sudo bluetoothctl power on
sudo bluetoothctl show
```

Ensure the adapter is powered and the inverters are in range. Close any phone
app connected to them. Use `sudo bluetoothctl scan on` interactively to check
advertisements, then `scan off` and `quit`. The project does not require pairing
as part of its normal BLE flow.

Create the database (the server must already be running):

```sh
curl --fail-with-body -X POST http://127.0.0.1:8086/query \
  --data-urlencode 'q=CREATE DATABASE solar'
```

Check the JSON response for an error as InfluxDB can return query errors with
HTTP 200. The collector does not automatically create databases.

Build in root's Podman image store, then install the configuration:

```sh
sudo podman build -t localhost/delta-solar:latest -f Dockerfile .
sudo install -m 0644 solar-inverters.env.example /etc/solar-inverters.env
sudo install -d /etc/containers/systemd
sudo install -m 0644 solar-inverters.container /etc/containers/systemd/
sudo systemctl daemon-reload
sudo systemctl start solar-inverters.service
sudo journalctl -u solar-inverters.service -f
```

Quadlet's `[Install]` section arranges boot startup when the generator runs;
do not `systemctl enable` the generated service. The named Podman volume
`solar-inverters-data` persists queued readings across rebuilds and restarts.
After rebuilding an image, run `sudo systemctl restart solar-inverters.service`.

Host networking makes `127.0.0.1:8086` reach the Pi's InfluxDB. Bluetooth uses
the host's D-Bus socket, not a `/dev` device. No privileged flag or container
Bluetooth daemon is needed. Root inside this rootful container is used for
the host BlueZ D-Bus policy. The D-Bus mount grants access to host services;
its read-only flag does not make D-Bus calls read-only.

## Data and timing

Measurement: `inverter_data`. Tags: `serial`, `model=M4-TL-US`.

Fields follow upstream naming: `Power` (kW), `DailyEnergy` (kWh), `ACVoltage`
(V), `ACCurrent` (A), `ACFrequency` (Hz), and `PV1` through `PV4` voltage/current
(V/A) where supported. Unsupported responses are omitted; valid zeroes are
retained. Two samples per field are averaged. Negative values are omitted
for these nonnegative production measurements.

`DailyEnergy` is the inverter's counter as decoded upstream; its actual reset
behavior must be verified on these devices. This version does not manufacture
`TodaysEnergy` or apply upstream's hardcoded generation limits. Old dashboards
using derived fields need adjustment. Filter/group by `serial` to distinguish
inverters.

Timestamps are UTC nanoseconds, taken at the start of each inverter read.
`TZ=America/Los_Angeles` configures the container's local timezone; logs and
stored timestamps remain UTC. Set dashboards to America/Los_Angeles for
Pacific-time display and use timezone-aware day boundaries for daily totals.

Each inverter has an independent Tokio polling task with a default **60-second
minimum interval**. `POLL_MODE=sequential` (the default) polls one inverter at a
time. `POLL_MODE=concurrent` allows overlapping reads while a shared radio
coordinator serializes discovery, connection attempts, and disconnects. A shared
radio coordinator serializes discovery, connection attempts, and disconnects.
Successfully connected peripherals are cached, so the next cycle skips the full
scan when the inverter is still advertising from the previous read.

Each polling cycle allows up to **three attempts**, with randomized backoff of
2–5 seconds before the second attempt and 4–7 seconds before the third. A shared
90-second per-inverter budget (`POLL_TIMEOUT_SECONDS`) covers radio-coordinator
waits, discovery, connection, reads, and retry backoff. Failed reads and
connection attempts are retried; partial failed reads are discarded rather than
merged across attempts. The next cycle starts after the configured interval, or
at least one second after an overlong cycle finishes. Cycles for the **same**
inverter never overlap.

`POLL_MODE=sequential` avoids overlapping Bluetooth sessions and is recommended
for this Pi and inverter combination. Concurrent reads cannot overcome another
client holding an inverter's only BLE connection, but they add variable connection
contention.

Bluetooth timing can be tuned with environment variables:

| Variable | Default | Effect |
|---|---|---|
| `SCAN_DURATION_SECONDS` | `2` | Seconds to scan for inverter advertisements per connection attempt |
| `INTER_COMMAND_SLEEP_MS` | `100` | Milliseconds between each GATT command during a read |
| `DISCONNECT_TIMEOUT_SECONDS` | `3` | Maximum seconds to wait for Bluetooth disconnect cleanup |

The previous defaults (5s scan, 500ms inter-command sleep, 5s disconnect) added
about 23s of fixed overhead per read. Current defaults cut that to roughly 7–9s.
If you see `No response to command` warnings, increase `INTER_COMMAND_SLEEP_MS`
or `POLL_TIMEOUT_SECONDS`.

Logs include the inverter serial, attempt number, complete error chain, and a
per-inverter status summary (`cycles`, `successes`, `seconds_since_success`).
`seconds_since_success=None` means no reading has succeeded since this process
started. Counts describe whole polling cycles, not individual connection attempts.

Each queued-measurement log includes:
- **`complete=true`**: every requested command received a response.
- **`complete=false`**: at least one command timed out; remaining samples still
  upload if any measurements were received.
- **`samples=name:count,...`**: valid samples received per field. Two samples per
  field are requested; fewer means partial coverage from transient command loss.
- **`commands_timed_out=N`**: number of commands that received no response.
Debug logging adds discovery, connection, read, and disconnect stages. Field
values in logs are rounded to three decimal places; stored precision is unchanged.

Every completed read is written to the spool before upload. Failed HTTP writes
retain the original line and timestamp for retry; resending a point is
idempotent for the same series/timestamp. A **single independent upload worker**
handles both producers, so uploads do not block Bluetooth polling or race over
queued files. It wakes on new readings or every ten seconds, processes up to
100 directory entries per pass, and stops the pass on an upload error. Failures
have a minimum ten-second retry delay. Successful uploads are logged individually.
The spool has no automatic
expiry or size limit: monitor the volume's disk use during prolonged outages.
Incomplete `.tmp` files left by abrupt termination are not uploaded.

## Verify collection

```sh
curl --fail-with-body -G http://127.0.0.1:8086/query \
  --data-urlencode 'db=solar' \
  --data-urlencode 'q=SELECT LAST("Power") FROM "inverter_data" GROUP BY "serial"'
```

Expect separate series for `1010` and `1006`. Nighttime/offline inverters may
not advertise or respond. For troubleshooting, set
`RUST_LOG=delta_solar=debug,btleplug=debug` in `/etc/solar-inverters.env` and
restart the service. Check `sudo bluetoothctl show`, `sudo systemctl status
bluetooth`, and the journal for discovery, characteristic or command timeouts.

## Grafana dashboard

Import [`grafana/solar-dashboard.json`](grafana/solar-dashboard.json) into
**Grafana 13.2.1** using **Dashboards → New → Import → Upload dashboard JSON file**.
The file uses Grafana's classic dashboard format and built-in panels; no additional
plugins are needed.

First create an **InfluxDB** datasource under **Connections → Data sources**:

| Setting | Value |
| --- | --- |
| Query language | **InfluxQL** |
| URL | `http://<pi-hostname-or-ip>:8086` |
| Database | `solar` (or your `INFLUX_DATABASE` value) |
| User/password | Leave empty for the project's default unauthenticated server |
| HTTP method | GET |
| Min time interval | `1m` |

The URL must be reachable **from the Grafana server**. Use `127.0.0.1` only if
Grafana shares the InfluxDB host's network namespace; a separate Grafana container
usually needs the Pi's hostname or IP instead. Click **Save & test**, then choose
that datasource for **Solar InfluxDB** during dashboard import.

The dashboard includes:

- Recent power per inverter and reported energy counters.
- Per-inverter and stacked production charts, plus energy-counter trends.
- AC voltage, current, and frequency.
- PV1–PV4 voltage and current panels repeated for each selected inverter.
- Last Power sample timestamps and Power sample counts per five-minute bucket.

The **Inverter** selector discovers serial tags automatically and defaults to all
inverters. Time display is `America/Los_Angeles`, the initial range is 24 hours,
and refresh is every minute. Inverters 1006 and 1010 have consistent blue and
orange colors. The **recent power** card always queries the last five minutes of
wall-clock time, even when viewing a historical date range; other panels follow
the date picker. An inverter without recent data may disappear from that card.

`DailyEnergy` is labeled **Reported energy counter** because its reset behavior
has not been verified. It is not summed into a daily yield. Measurement gaps are
left empty, and unsupported PV channels show no data. The stacked power chart
shows only observed production and can be incomplete when an inverter is missing.
Sample counts measure stored `Power` fields, not polling success rate; zero counts
do not imply zero production. Delayed uploads retain original timestamps and can
backfill historical gaps. Collector retry/error metrics are available in logs,
not in this measurement.

If panels are empty, verify the datasource database and widen the time range
(inverters may be offline at night). The query in **Verify collection** above
checks whether `Power` exists for both serials. Use a panel's **Inspect → Query**
to troubleshoot datasource errors. The JSON uses an import-time `${DS_SOLAR}`
placeholder; for file provisioning instead of UI import, replace it with your
actual InfluxDB datasource UID and remove `__inputs`.

## Protocol status and development

The protocol is ported from [jat255/solar-inverter-datalogger](https://github.com/jat255/solar-inverter-datalogger).
The decoder validates CRCs, reassembles fragmented frames, and matches replies
to command IDs. Captured clock-setting commands are deliberately omitted to
avoid replaying upstream's fixed 2021 date/time. The captured access exchange
is retained. Both inverters have returned readings and uploaded successfully on
the Pi; sequential mode is the operational default based on hardware testing.
Concurrent mode remains available but is not recommended while overlapping
Bluetooth sessions produce intermittent connection issues.

```sh
cargo test --locked
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
```

The Docker build runs tests on Linux before compiling the release binary.
Build on the Pi for native ARM64, or use a builder configured for linux/arm64.
Local protocol tests do not establish that the device handshake works: final
acceptance requires connecting to both inverters on the Pi and confirming
separate measurements in InfluxDB.
