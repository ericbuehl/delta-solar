mod protocol;

use anyhow::{bail, Context, Result};
use btleplug::{
    api::{Central, CentralEvent, Manager as _, Peripheral as _, ScanFilter, WriteType},
    platform::{Adapter, Manager, Peripheral},
};
use futures::{FutureExt, Stream, StreamExt};
use std::{
    collections::BTreeMap,
    env,
    future::Future,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    fs,
    sync::{Mutex, Notify, Semaphore},
    time::{sleep, sleep_until, timeout, timeout_at, Instant},
};
use tracing::{debug, info, warn, Instrument};
use uuid::Uuid;

const WRITE: Uuid = Uuid::from_u128(0xee6d717188c611e8b444060400ef5315);
const NOTIFY: Uuid = Uuid::from_u128(0xee6d717288c611e8b444060400ef5315);
const MAX_ATTEMPTS: u32 = 5;
// Avoid spending the end of a cycle on a connection with little time left to read.
const MIN_ATTEMPT_BUDGET: Duration = Duration::from_secs(10);

#[derive(Default)]
struct RetryStats {
    attempts_used: u32,
    slot_wait_ms: u64,
    stop_reason: &'static str,
}

// Per-read quality metrics, logged alongside every polling cycle so partial
// reads are visible even when they still produce usable data.
#[derive(Default)]
struct ReadQuality {
    commands_requested: u32,
    commands_timed_out: u32,
    samples_by_field: BTreeMap<String, u32>,
}

// Per-phase timing for a single read cycle, used to tune command sleeps and
// response timeouts.
#[derive(Default)]
struct Timing {
    scan_ms: u64,
    connect_attempt_ms: u64,
    service_discovery_ms: u64,
    subscription_ms: u64,
    command_ms: u64,
    sleep_ms: u64,
    response_wait_ms: u64,
    disconnect_ms: u64,
    total_ms: u64,
    slow_commands: Vec<(u16, u16, u64)>,
}

struct Inverter {
    serial: String,
    mac: String,
}
struct Config {
    inverters: Vec<Inverter>,
    url: String,
    database: String,
    spool: PathBuf,
    interval: Duration,
    poll_timeout: Duration,
    adapter: usize,
    concurrent: bool,
    scan_duration_seconds: u64,
    inter_command_sleep_ms: u64,
    disconnect_timeout_seconds: u64,
}

fn setting(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.into())
}

impl Config {
    fn load() -> Result<Self> {
        let inverters_str =
            env::var("INVERTERS").context("INVERTERS env var is required (serial=MAC pairs)")?;
        let mut inverters = Vec::new();
        for entry in inverters_str.split(',') {
            let (serial, mac) = entry
                .split_once('=')
                .context("INVERTERS must contain serial=MAC pairs")?;
            if serial.is_empty()
                || !serial
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-')
            {
                bail!("Invalid serial");
            }
            mac.parse::<btleplug::api::BDAddr>()
                .context("Invalid Bluetooth MAC")?;
            if inverters
                .iter()
                .any(|i: &Inverter| i.serial == serial || i.mac == mac.to_uppercase())
            {
                bail!("Duplicate inverter");
            }
            inverters.push(Inverter {
                serial: serial.into(),
                mac: mac.to_uppercase(),
            });
        }
        let interval: u64 = setting("POLL_INTERVAL_SECONDS", "60").parse()?;
        let poll_timeout: u64 = setting("POLL_TIMEOUT_SECONDS", "45").parse()?;
        let scan_duration: u64 = setting("SCAN_DURATION_SECONDS", "5").parse()?;
        let inter_command_sleep: u64 = setting("INTER_COMMAND_SLEEP_MS", "100").parse()?;
        let disconnect_timeout: u64 = setting("DISCONNECT_TIMEOUT_SECONDS", "5").parse()?;
        if interval == 0 || poll_timeout == 0 {
            bail!("Polling durations must be positive");
        }
        if scan_duration == 0 {
            bail!("SCAN_DURATION_SECONDS must be positive");
        }
        if inter_command_sleep == 0 {
            bail!("INTER_COMMAND_SLEEP_MS must be positive");
        }
        if disconnect_timeout == 0 {
            bail!("DISCONNECT_TIMEOUT_SECONDS must be positive");
        }
        Ok(Self {
            inverters,
            url: setting("INFLUX_URL", "http://127.0.0.1:8086"),
            database: setting("INFLUX_DATABASE", "solar"),
            spool: setting("SPOOL_DIR", "/var/lib/delta-solar").into(),
            interval: Duration::from_secs(interval),
            poll_timeout: Duration::from_secs(poll_timeout),
            adapter: setting("BLUETOOTH_ADAPTER_INDEX", "0").parse()?,
            concurrent: match setting("POLL_MODE", "sequential").as_str() {
                "concurrent" => true,
                "sequential" => false,
                _ => bail!("POLL_MODE must be concurrent or sequential"),
            },
            scan_duration_seconds: scan_duration,
            inter_command_sleep_ms: inter_command_sleep,
            disconnect_timeout_seconds: disconnect_timeout,
        })
    }
}

async fn collect(
    config: &Config,
    device: &Peripheral,
    timing: &mut Timing,
) -> Result<(BTreeMap<String, f64>, ReadQuality)> {
    debug!(stage = "services", "Discovering GATT services");
    let discovery_start = Instant::now();
    device
        .discover_services()
        .await
        .context("Discover GATT services")?;
    timing.service_discovery_ms = discovery_start.elapsed().as_millis() as u64;

    let chars = device.characteristics();
    let write = chars
        .iter()
        .find(|c| c.uuid == WRITE)
        .context("Delta write characteristic missing")?;
    let notify = chars
        .iter()
        .find(|c| c.uuid == NOTIFY)
        .context("Delta notify characteristic missing")?;
    let sub_start = Instant::now();
    let mut notifications = device.notifications().await?;
    device
        .subscribe(notify)
        .await
        .context("Subscribe to measurements")?;
    timing.subscription_ms = sub_start.elapsed().as_millis() as u64;

    debug!(stage = "read", "Reading measurements");
    let mut decoder = protocol::Decoder::default();
    let mut samples: BTreeMap<String, Vec<f64>> = BTreeMap::new();
    let mut quality = ReadQuality::default();
    // Upstream's access command, without replaying its captured clock-setting writes.
    let mut commands = vec![vec![0, 9], vec![120, 5], vec![100, 253, 25, 132]];
    for _ in 0..2 {
        commands.extend(protocol::FIELDS.iter().map(|(_, a, b, _)| vec![*a, *b]));
    }
    let command_loop_start = Instant::now();
    for payload in commands {
        quality.commands_requested += 1;
        let write_start = Instant::now();
        device
            .write(write, &protocol::command(&payload), WriteType::WithResponse)
            .await?;

        let response_start = Instant::now();
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut matched = false;
        loop {
            let notification = match timeout(
                deadline.saturating_duration_since(Instant::now()),
                notifications.next(),
            )
            .await
            {
                Ok(Some(n)) => n,
                Ok(None) => bail!("Bluetooth notification stream ended"),
                Err(_) => {
                    quality.commands_timed_out += 1;
                    let elapsed = response_start.elapsed().as_millis() as u64;
                    timing.response_wait_ms += elapsed;
                    warn!(
                        command = ?payload,
                        response_ms = elapsed,
                        "No response to command"
                    );
                    break;
                }
            };
            if notification.uuid != NOTIFY {
                continue;
            }
            for frame in decoder.push(&notification.value) {
                if frame[4..6] != payload[..2] {
                    continue;
                }
                matched = true;
                if let Some(raw) = protocol::value(&frame) {
                    if raw < 0 {
                        continue;
                    }
                    if let Some((name, _, _, scale)) = protocol::FIELDS
                        .iter()
                        .find(|(_, a, b, _)| *a == frame[4] && *b == frame[5])
                    {
                        let elapsed = response_start.elapsed().as_millis() as u64;
                        timing.response_wait_ms += elapsed;
                        debug!(
                            command = ?payload,
                            response_ms = elapsed,
                            field = %name,
                            raw = raw,
                            "Command response"
                        );
                        *quality.samples_by_field.entry((*name).into()).or_insert(0) += 1;
                        samples
                            .entry((*name).into())
                            .or_default()
                            .push(raw as f64 / scale);
                    }
                }
            }
            if matched {
                break;
            }
        }

        let sleep_start = Instant::now();
        sleep(Duration::from_millis(config.inter_command_sleep_ms)).await;
        timing.sleep_ms += sleep_start.elapsed().as_millis() as u64;

        if payload == [0, 9] {
            // Nonstandard access packet captured by upstream immediately after
            // SerialNumber. Its response is not a normal Delta framed reading.
            device
                .write(
                    write,
                    &[1, 101, 180, 52, 0, 1, 234, 60],
                    WriteType::WithResponse,
                )
                .await?;
            let access_sleep_start = Instant::now();
            sleep(Duration::from_millis(config.inter_command_sleep_ms)).await;
            timing.sleep_ms += access_sleep_start.elapsed().as_millis() as u64;
        }

        // Track slow commands (>1 s end-to-end) for tuning.
        let command_elapsed = write_start.elapsed().as_millis() as u64;
        if command_elapsed > 1000 {
            timing
                .slow_commands
                .push((payload[0] as u16, payload[1] as u16, command_elapsed));
        }
    }
    timing.command_ms += command_loop_start.elapsed().as_millis() as u64;

    if samples.is_empty() {
        bail!("No usable measurements received");
    }
    let fields = samples
        .into_iter()
        .map(|(name, values)| (name, values.iter().sum::<f64>() / values.len() as f64))
        .collect();
    Ok((fields, quality))
}

fn line(serial: &str, fields: &BTreeMap<String, f64>, timestamp: i64) -> String {
    let fields = fields
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",");
    format!("inverter_data,model=M4-TL-US,serial={serial} {fields} {timestamp}\n")
}

async fn enqueue(dir: &Path, line: &str) -> Result<()> {
    let id = Uuid::new_v4();
    let temporary = dir.join(format!("{id}.tmp"));
    let final_path = dir.join(format!("{id}.lp"));
    use tokio::io::AsyncWriteExt;
    let mut file = fs::File::create(&temporary).await?;
    file.write_all(line.as_bytes()).await?;
    file.sync_all().await?;
    drop(file);
    fs::rename(temporary, final_path).await?;
    Ok(())
}

async fn flush(client: &reqwest::Client, config: &Config) -> Result<()> {
    let mut entries = fs::read_dir(&config.spool).await?;
    // Bound each flush so a large backlog cannot starve Bluetooth collection.
    for _ in 0..100 {
        let Some(entry) = entries.next_entry().await? else {
            break;
        };
        if entry.path().extension().and_then(|s| s.to_str()) != Some("lp") {
            continue;
        }
        let body = fs::read(entry.path()).await?;
        let response = client
            .post(format!("{}/write", config.url.trim_end_matches('/')))
            .query(&[("db", config.database.as_str()), ("precision", "ns")])
            .header("Content-Type", "text/plain")
            .body(body)
            .send()
            .await?;
        if !response.status().is_success() {
            bail!("InfluxDB {}: {}", response.status(), response.text().await?);
        }
        fs::remove_file(entry.path()).await?;
        info!(file = %entry.file_name().to_string_lossy(), "Uploaded queued reading to InfluxDB");
    }
    Ok(())
}

// BlueZ's events() starts with synthetic events for already-known peripherals.
// Drain those before starting discovery; they are not evidence of a fresh sighting.
fn discard_ready_events<S: Stream + Unpin>(events: &mut S) -> Result<()> {
    loop {
        match events.next().now_or_never() {
            Some(Some(_)) => {}
            Some(None) => bail!("Bluetooth discovery event stream ended"),
            None => return Ok(()),
        }
    }
}

async fn wait_for_target<S, T>(events: &mut S, target: &str) -> Result<T>
where
    S: Stream<Item = Option<(String, T)>> + Unpin,
{
    while let Some(event) = events.next().await {
        if let Some((address, device)) = event {
            if address == target {
                return Ok(device);
            }
        }
    }
    bail!("Bluetooth discovery event stream ended before target was observed")
}

// One coordinator owns adapter-wide discovery and connection establishment.
// The guard is released during GATT reads, allowing readings to overlap.
struct Radio {
    adapter: Adapter,
    ready_at: Mutex<Instant>,
}

impl Radio {
    async fn connect(
        &self,
        config: &Config,
        inverter: &Inverter,
        deadline: Instant,
        timing: &mut Timing,
    ) -> Result<Peripheral> {
        let mut ready_at = timeout_at(deadline, self.ready_at.lock())
            .await
            .context("Timed out waiting for radio coordinator")?;
        timeout_at(deadline, sleep_until(*ready_at))
            .await
            .context("Radio cooldown exceeded polling budget")?;

        debug!(stage = "discovery", "Refreshing Bluetooth discovery");
        let scan_start = Instant::now();
        let discovered = timeout_at(
            deadline.min(Instant::now() + Duration::from_secs(config.scan_duration_seconds)),
            async {
                let mut events = self
                    .adapter
                    .events()
                    .await
                    .context("Subscribe to discovery events")?;
                discard_ready_events(&mut events)?;
                self.adapter
                    .start_scan(ScanFilter::default())
                    .await
                    .context("Start discovery")?;
                let sightings = events.then(|event| async {
                    let id = match event {
                        CentralEvent::DeviceDiscovered(id)
                        | CentralEvent::DeviceUpdated(id)
                        | CentralEvent::ManufacturerDataAdvertisement { id, .. }
                        | CentralEvent::ServiceDataAdvertisement { id, .. }
                        | CentralEvent::ServicesAdvertisement { id, .. } => id,
                        _ => return None,
                    };
                    match self.adapter.peripheral(&id).await {
                        Ok(device) => Some((device.address().to_string(), device)),
                        Err(error) => {
                            debug!(%error, "Discovery peripheral disappeared before lookup");
                            None
                        }
                    }
                });
                futures::pin_mut!(sightings);
                wait_for_target(&mut sightings, &inverter.mac).await
            },
        )
        .await;
        // Always stop discovery, including when subscription/start/sighting timed out.
        // Keep the coordinator locked until cleanup completes.
        timeout(Duration::from_secs(3), self.adapter.stop_scan())
            .await
            .context("Stop discovery timed out")?
            .context("Stop discovery failed")?;
        timing.scan_ms = scan_start.elapsed().as_millis() as u64;
        let target_observed = matches!(&discovered, Ok(Ok(_)));
        // RSSI is diagnostic only. Missing properties must not fail discovery,
        // and the lookup shares the polling deadline with a short local cap.
        let rssi_dbm = match &discovered {
            Ok(Ok(device)) => match timeout_at(
                deadline.min(Instant::now() + Duration::from_millis(250)),
                device.properties(),
            )
            .await
            {
                Ok(Ok(properties)) => properties.and_then(|properties| properties.rssi),
                Ok(Err(error)) => {
                    debug!(%error, "Target RSSI unavailable");
                    None
                }
                Err(_) => {
                    debug!("Target RSSI lookup timed out");
                    None
                }
            },
            _ => None,
        };
        info!(
            stage = "discovery",
            target_observed,
            scan_ms = timing.scan_ms,
            rssi_dbm = ?rssi_dbm,
            "Bluetooth discovery finished"
        );
        let device = discovered
            .context("Discovery timed out waiting for a fresh target sighting")?
            .context("Discover target inverter")?;
        if Instant::now() >= deadline {
            bail!("Polling budget expired after discovery cleanup");
        }
        debug!(stage = "connect", "Establishing Bluetooth connection");
        let connect_start = Instant::now();
        let connected = timeout_at(
            deadline.min(Instant::now() + Duration::from_secs(10)),
            device.connect(),
        )
        .await;
        timing.connect_attempt_ms = connect_start.elapsed().as_millis() as u64;
        match connected {
            Ok(Ok(())) => Ok(device),
            result => {
                let error = match result {
                    Ok(Err(error)) => {
                        anyhow::Error::new(error).context("Bluetooth connection failed")
                    }
                    Err(error) => {
                        anyhow::Error::new(error).context("Bluetooth connection timed out")
                    }
                    _ => unreachable!(),
                };
                warn!(stage = "connect", connection_ms = timing.connect_attempt_ms, rssi_dbm = ?rssi_dbm,
                    error = %format!("{error:#}"), "Bluetooth connection attempt failed");
                disconnect(&device, config.disconnect_timeout_seconds).await;
                *ready_at = Instant::now() + Duration::from_secs(2);
                Err(error)
            }
        }
    }

    async fn read(
        &self,
        config: &Config,
        inverter: &Inverter,
        deadline: Instant,
    ) -> Result<(BTreeMap<String, f64>, ReadQuality, Timing)> {
        let cycle_start = Instant::now();

        let mut timing = Timing::default();
        let device = self
            .connect(config, inverter, deadline, &mut timing)
            .await?;
        info!(
            connection_ms = timing.connect_attempt_ms,
            scan_ms = timing.scan_ms,
            "Bluetooth connected"
        );

        let result = timeout_at(deadline, collect(config, &device, &mut timing))
            .await
            .context("Measurement read timed out");

        // Cleanup is outside the read timeout. Serializing it avoids overlapping
        // radio control operations and enforces a cooldown before the next connect.
        let mut ready_at = self.ready_at.lock().await;
        let disconnect_start = Instant::now();
        let disconnected = disconnect(&device, config.disconnect_timeout_seconds).await;
        let disconnect_ms = disconnect_start.elapsed().as_millis() as u64;
        debug!(disconnected, disconnect_ms, "Bluetooth cleanup finished");
        *ready_at = Instant::now() + Duration::from_secs(2);

        let (fields, quality) = result?.context("Collect measurements")?;
        timing.disconnect_ms = disconnect_ms;
        timing.total_ms = cycle_start.elapsed().as_millis() as u64;
        Ok((fields, quality, timing))
    }
}

async fn disconnect(device: &Peripheral, timeout_seconds: u64) -> bool {
    info!(stage = "disconnect", "Disconnecting Bluetooth");
    match timeout(Duration::from_secs(timeout_seconds), device.disconnect()).await {
        Ok(Ok(())) => true,
        Ok(Err(error)) => {
            warn!(%error, "Bluetooth disconnect failed");
            false
        }
        Err(_) => {
            warn!("Bluetooth disconnect timed out");
            false
        }
    }
}

fn retry_delay(attempt: u32) -> Duration {
    // UUID's random bits avoid synchronized retries without another RNG dependency.
    let jitter_ms = (Uuid::new_v4().as_u128() % 3001) as u64;
    Duration::from_millis(2000 * u64::from(attempt) + jitter_ms)
}

// The operation must bound its own I/O by deadline, then finish cleanup. Do not
// cancel the entire operation here: that could leave a scan/connection active.
async fn retry_read<T, F, Fut>(
    deadline: Instant,
    slots: &Semaphore,
    stats: &mut RetryStats,
    mut operation: F,
) -> Result<T>
where
    F: FnMut(Instant) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut last_error = anyhow::anyhow!("Polling budget expired");
    for attempt in 1..=MAX_ATTEMPTS {
        if Instant::now() >= deadline {
            stats.stop_reason = "deadline";
            break;
        }
        if deadline.saturating_duration_since(Instant::now()) < MIN_ATTEMPT_BUDGET {
            stats.stop_reason = "insufficient_budget";
            break;
        }
        let wait_started = Instant::now();
        let slot = timeout_at(deadline, slots.acquire()).await;
        stats.slot_wait_ms += wait_started.elapsed().as_millis() as u64;
        let slot = match slot {
            Ok(slot) => slot.context("Polling semaphore closed")?,
            Err(error) => {
                stats.stop_reason = "deadline";
                return Err(error).context("Polling budget expired waiting for polling slot");
            }
        };
        // Slot contention counts against the same deadline as I/O and backoff.
        if deadline.saturating_duration_since(Instant::now()) < MIN_ATTEMPT_BUDGET {
            stats.stop_reason = if Instant::now() >= deadline {
                "deadline"
            } else {
                "insufficient_budget"
            };
            break;
        }
        stats.attempts_used += 1;
        info!(
            attempt,
            budget_remaining_ms = deadline
                .saturating_duration_since(Instant::now())
                .as_millis(),
            "Starting read attempt"
        );
        let result = operation(deadline).await;
        // Keep the complete Bluetooth session sequential, but let another inverter
        // use the slot while this one backs off. Cleanup finishes inside operation.
        drop(slot);
        match result {
            Ok(value) => {
                stats.stop_reason = "success";
                return Ok(value);
            }
            Err(error) => {
                warn!(attempt, error = %format!("{error:#}"), "Read attempt failed");
                last_error = error;
            }
        }
        if Instant::now() >= deadline {
            stats.stop_reason = "deadline";
            break;
        }
        if attempt == MAX_ATTEMPTS {
            stats.stop_reason = "attempt_limit";
            break;
        }
        let delay = retry_delay(attempt);
        if Instant::now() + delay + MIN_ATTEMPT_BUDGET > deadline {
            stats.stop_reason = "insufficient_budget";
            break;
        }
        info!(delay_ms = delay.as_millis(), "Backing off before retry");
        sleep(delay).await;
    }
    Err(last_error)
}

async fn poll_inverter(
    config: &Config,
    inverter: &Inverter,
    radio: &Radio,
    slots: &Semaphore,
    uploads: &Notify,
) -> Result<()> {
    let mut last_success: Option<Instant> = None;
    let mut cycles = 0u64;
    let mut successes = 0u64;
    loop {
        let started = Instant::now();
        // Label the cycle consistently even when a retry connects in a later minute.
        let timestamp = chrono::Utc::now()
            .timestamp_nanos_opt()
            .expect("UTC timestamp fits i64");
        let mut retry_stats = RetryStats::default();
        let result = retry_read(
            started + config.poll_timeout,
            slots,
            &mut retry_stats,
            |deadline| radio.read(config, inverter, deadline),
        )
        .await;
        cycles += 1;
        match result {
            Ok((fields, quality, timing)) => {
                enqueue(&config.spool, &line(&inverter.serial, &fields, timestamp)).await?;
                uploads.notify_one();
                last_success = Some(Instant::now());
                successes += 1;
                let complete = quality.commands_timed_out == 0;
                let samples: Vec<String> = quality
                    .samples_by_field
                    .iter()
                    .map(|(name, count)| format!("{name}:{count}"))
                    .collect();
                let slow: Vec<String> = timing
                    .slow_commands
                    .iter()
                    .map(|(a, b, ms)| format!("{a},{b}:{ms}ms"))
                    .collect();
                let formatted_fields = fields
                    .iter()
                    .map(|(name, value)| format!("{name}={value:.3}"))
                    .collect::<Vec<_>>()
                    .join(" ");
                info!(
                    fields = %formatted_fields,
                    samples = %samples.join(","),
                    commands_timed_out = quality.commands_timed_out,
                    complete = complete,
                    total_ms = timing.total_ms,
                    connection_ms = timing.connect_attempt_ms,
                    scan_ms = timing.scan_ms,
                    service_discovery_ms = timing.service_discovery_ms,
                    subscription_ms = timing.subscription_ms,
                    command_ms = timing.command_ms,
                    sleep_ms = timing.sleep_ms,
                    response_wait_ms = timing.response_wait_ms,
                    disconnect_ms = timing.disconnect_ms,
                    slow_commands = ?slow,
                    "Queued measurements",
                );
            }
            Err(error) => warn!(error = %format!("{error:#}"), "Polling cycle failed"),
        }
        info!(cycles, successes, seconds_since_success = ?last_success.map(|time| time.elapsed().as_secs()),
            cycle_ms = started.elapsed().as_millis(), attempts_used = retry_stats.attempts_used,
            slot_wait_ms = retry_stats.slot_wait_ms, stop_reason = retry_stats.stop_reason,
            "Inverter polling status");
        sleep(
            config
                .interval
                .saturating_sub(started.elapsed())
                .max(Duration::from_secs(1)),
        )
        .await;
    }
}

async fn upload_worker(client: &reqwest::Client, config: &Config, uploads: &Notify) -> Result<()> {
    loop {
        if let Err(error) = flush(client, config).await {
            warn!(%error, "Upload failed; queued readings retained");
            // New readings cannot turn a database outage into a tight retry loop.
            sleep(Duration::from_secs(10)).await;
        }
        tokio::select! {
            _ = uploads.notified() => {},
            _ = sleep(Duration::from_secs(10)) => {},
        }
    }
}

async fn run(config: Config) -> Result<()> {
    fs::create_dir_all(&config.spool).await?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let manager = Manager::new().await?;
    let adapters = manager.adapters().await?;
    let adapter = adapters
        .get(config.adapter)
        .context("Bluetooth adapter not found; check host BlueZ and adapter index")?;
    info!(adapter = %adapter.adapter_info().await?, concurrent = config.concurrent,
        poll_timeout_seconds = config.poll_timeout.as_secs(), max_attempts = MAX_ATTEMPTS,
        scan_duration_seconds = config.scan_duration_seconds,
        disconnect_timeout_seconds = config.disconnect_timeout_seconds, "Starting collector");
    let radio = Radio {
        adapter: adapter.clone(),
        ready_at: Mutex::new(Instant::now()),
    };
    let slots = Semaphore::new(if config.concurrent {
        config.inverters.len()
    } else {
        1
    });
    let uploads = Notify::new();
    let workers = futures::future::try_join_all(config.inverters.iter().map(|inverter| {
        poll_inverter(&config, inverter, &radio, &slots, &uploads)
            .instrument(tracing::info_span!("inverter", serial = %inverter.serial))
    }));
    tokio::select! {
        result = workers => { result?; Ok(()) },
        result = upload_worker(&client, &config, &uploads) => result,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(setting("RUST_LOG", "delta_solar=info"))
        .with_ansi(false)
        .init();
    let config = Config::load()?;
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        result = run(config) => result,
        _ = async {
            #[cfg(unix)]
            tokio::select! { _ = terminate.recv() => {}, _ = tokio::signal::ctrl_c() => {} }
            #[cfg(not(unix))]
            let _ = tokio::signal::ctrl_c().await;
        } => { info!("Shutting down"); Ok(()) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test(start_paused = true)]
    async fn retrying_inverter_does_not_block_healthy_one() {
        let failures = AtomicUsize::new(0);
        let healthy = AtomicUsize::new(0);
        let active = AtomicUsize::new(0);
        let slots = Semaphore::new(1);
        let mut retrying_stats = RetryStats::default();
        let mut healthy_stats = RetryStats::default();
        let deadline = Instant::now() + Duration::from_secs(45);
        let retrying = retry_read(deadline, &slots, &mut retrying_stats, |_| async {
            assert_eq!(active.fetch_add(1, Ordering::SeqCst), 0);
            let attempt = failures.fetch_add(1, Ordering::SeqCst);
            sleep(Duration::from_secs(1)).await;
            assert_eq!(active.fetch_sub(1, Ordering::SeqCst), 1);
            if attempt < 2 {
                bail!("Simulated connection contention");
            }
            assert_eq!(healthy.load(Ordering::SeqCst), 1);
            Ok("example-a")
        });
        let working = retry_read(deadline, &slots, &mut healthy_stats, |_| async {
            assert_eq!(active.fetch_add(1, Ordering::SeqCst), 0);
            // This inverter must run after the first failure, during its backoff.
            assert_eq!(failures.load(Ordering::SeqCst), 1);
            sleep(Duration::from_secs(1)).await;
            healthy.fetch_add(1, Ordering::SeqCst);
            assert_eq!(active.fetch_sub(1, Ordering::SeqCst), 1);
            Ok("example-b")
        });
        let (retrying, working) = tokio::join!(retrying, working);
        assert_eq!(retrying.unwrap(), "example-a");
        assert_eq!(working.unwrap(), "example-b");
        assert_eq!(failures.load(Ordering::SeqCst), 3);
        assert_eq!(retrying_stats.stop_reason, "success");
        assert_eq!(healthy_stats.slot_wait_ms, 1000);
        assert_eq!(slots.available_permits(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn retries_stop_at_attempt_limit_or_budget() {
        for budget in [1, 90] {
            let attempts = AtomicUsize::new(0);
            let slots = Semaphore::new(1);
            let mut stats = RetryStats::default();
            let started = Instant::now();
            let result: Result<()> = retry_read(
                started + Duration::from_secs(budget),
                &slots,
                &mut stats,
                |_| async {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    bail!("Unavailable");
                },
            )
            .await;
            assert!(result.is_err());
            assert_eq!(
                attempts.load(Ordering::SeqCst),
                if budget == 1 {
                    0
                } else {
                    MAX_ATTEMPTS as usize
                }
            );
            assert!(started.elapsed() < Duration::from_secs(budget));
            assert_eq!(
                stats.stop_reason,
                if budget == 1 {
                    "insufficient_budget"
                } else {
                    "attempt_limit"
                }
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn slot_wait_uses_budget_and_does_not_start_a_late_attempt() {
        for release_after in [15, 25] {
            let slots = Semaphore::new(1);
            let held = slots.acquire().await.unwrap();
            let mut stats = RetryStats::default();
            let started = Instant::now();
            let waiting = async {
                let result: Result<()> = retry_read(
                    started + Duration::from_secs(20),
                    &slots,
                    &mut stats,
                    |_| async { panic!("No useful read budget remains after waiting") },
                )
                .await;
                assert!(result.is_err());
                assert_eq!(
                    started.elapsed(),
                    Duration::from_secs(release_after.min(20))
                );
            };
            let releasing = async {
                sleep(Duration::from_secs(release_after)).await;
                drop(held);
            };
            tokio::join!(waiting, releasing);
            assert_eq!(stats.attempts_used, 0);
            assert_eq!(stats.slot_wait_ms, release_after.min(20) * 1000);
            assert_eq!(
                stats.stop_reason,
                if release_after < 20 {
                    "insufficient_budget"
                } else {
                    "deadline"
                }
            );
            assert_eq!(slots.available_permits(), 1);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn retry_budget_is_not_reset_after_a_failed_attempt() {
        let slots = Semaphore::new(1);
        let mut stats = RetryStats::default();
        let started = Instant::now();
        let deadline = started + Duration::from_secs(45);
        let result: Result<()> = retry_read(
            deadline,
            &slots,
            &mut stats,
            |attempt_deadline| async move {
                assert_eq!(attempt_deadline, deadline);
                sleep_until(attempt_deadline).await;
                // Operations finish bounded cleanup before releasing the slot.
                sleep(Duration::from_secs(2)).await;
                bail!("Read timed out, cleanup finished");
            },
        )
        .await;
        assert!(result.is_err());
        assert_eq!(stats.attempts_used, 1);
        assert_eq!(stats.stop_reason, "deadline");
        assert_eq!(started.elapsed(), Duration::from_secs(47));
        assert_eq!(slots.available_permits(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_is_skipped_when_it_would_leave_too_little_read_time() {
        let slots = Semaphore::new(1);
        let mut stats = RetryStats::default();
        let started = Instant::now();
        let result: Result<()> = retry_read(
            started + Duration::from_secs(20),
            &slots,
            &mut stats,
            |_| async {
                sleep(Duration::from_secs(9)).await;
                bail!("Failed with eleven seconds remaining");
            },
        )
        .await;
        assert!(result.is_err());
        // Even the shortest backoff (2 s) would leave less than the 10 s minimum.
        assert_eq!(started.elapsed(), Duration::from_secs(9));
        assert_eq!(stats.attempts_used, 1);
        assert_eq!(stats.stop_reason, "insufficient_budget");
    }

    #[tokio::test(start_paused = true)]
    async fn discovery_ignores_initial_devices_and_waits_for_fresh_target() -> Result<()> {
        let (sender, mut sightings) = futures::channel::mpsc::unbounded();
        // BlueZ reports known peripherals immediately when events() is opened.
        sender.unbounded_send(Some(("target".into(), "stale handle")))?;
        discard_ready_events(&mut sightings)?;
        let advertising = async {
            sleep(Duration::from_secs(1)).await;
            sender.unbounded_send(None).unwrap();
            sender
                .unbounded_send(Some(("other".into(), "wrong device")))
                .unwrap();
            sleep(Duration::from_secs(1)).await;
            sender
                .unbounded_send(Some(("target".into(), "fresh handle")))
                .unwrap();
        };
        let (device, ()) = tokio::join!(wait_for_target(&mut sightings, "target"), advertising);
        assert_eq!(device?, "fresh handle");
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn discovery_times_out_without_target_and_reports_closed_stream() -> Result<()> {
        let (sender, mut sightings) = futures::channel::mpsc::unbounded();
        sender.unbounded_send(Some(("other".into(), "wrong device")))?;
        let started = Instant::now();
        let result = timeout(
            Duration::from_secs(5),
            wait_for_target(&mut sightings, "target"),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(started.elapsed(), Duration::from_secs(5));
        drop(sender);
        assert!(wait_for_target(&mut sightings, "target").await.is_err());
        assert!(discard_ready_events(&mut sightings).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_producers_preserve_distinct_spool_entries() -> Result<()> {
        let spool = env::temp_dir().join(format!("delta-solar-test-{}", Uuid::new_v4()));
        fs::create_dir(&spool).await?;
        let a = "inverter_data,serial=example-a Power=1 123\n";
        let b = "inverter_data,serial=example-b Power=2 123\n";
        tokio::try_join!(enqueue(&spool, a), enqueue(&spool, b))?;
        let mut entries = fs::read_dir(&spool).await?;
        let mut lines = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            assert_eq!(entry.path().extension().unwrap(), "lp");
            lines.push(fs::read_to_string(entry.path()).await?);
        }
        lines.sort();
        assert_eq!(lines, vec![a, b]);
        fs::remove_dir_all(&spool).await?;
        Ok(())
    }

    #[tokio::test]
    async fn upload_failure_retains_point_then_success_removes_it() -> Result<()> {
        let spool = env::temp_dir().join(format!("delta-solar-test-{}", Uuid::new_v4()));
        fs::create_dir(&spool).await?;
        let fields = BTreeMap::from([("Power".into(), 0.0)]);
        let point = line("example-a", &fields, 123456789);
        assert_eq!(
            point,
            "inverter_data,model=M4-TL-US,serial=example-a Power=0 123456789\n"
        );
        assert_ne!(point, line("example-b", &fields, 123456789));
        enqueue(&spool, &point).await?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            for status in ["503 Service Unavailable", "204 No Content"] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                loop {
                    let mut buffer = [0; 1024];
                    let length = socket.read(&mut buffer).await.unwrap();
                    assert_ne!(length, 0);
                    request.extend_from_slice(&buffer[..length]);
                    if request.ends_with(point.as_bytes()) {
                        break;
                    }
                }
                let request = String::from_utf8(request).unwrap();
                assert!(request.starts_with("POST /write?db=solar&precision=ns "));
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
            }
        });
        let config = Config {
            inverters: vec![],
            url,
            database: "solar".into(),
            spool: spool.clone(),
            interval: Duration::from_secs(60),
            poll_timeout: Duration::from_secs(45),
            adapter: 0,
            concurrent: true,
            scan_duration_seconds: 5,
            inter_command_sleep_ms: 100,
            disconnect_timeout_seconds: 5,
        };
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()?;
        assert!(flush(&client, &config).await.is_err());
        assert!(fs::read_dir(&spool).await?.next_entry().await?.is_some());
        flush(&client, &config).await?;
        assert!(fs::read_dir(&spool).await?.next_entry().await?.is_none());
        server.await?;
        fs::remove_dir(&spool).await?;
        Ok(())
    }
}
