mod protocol;

use anyhow::{bail, Context, Result};
use btleplug::{
    api::{Central, Manager as _, Peripheral as _, ScanFilter, WriteType},
    platform::{Adapter, Manager, Peripheral},
};
use futures::StreamExt;
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
        let poll_timeout: u64 = setting("POLL_TIMEOUT_SECONDS", "90").parse()?;
        let scan_duration: u64 = setting("SCAN_DURATION_SECONDS", "2").parse()?;
        let inter_command_sleep: u64 = setting("INTER_COMMAND_SLEEP_MS", "100").parse()?;
        let disconnect_timeout: u64 = setting("DISCONNECT_TIMEOUT_SECONDS", "3").parse()?;
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

// One coordinator owns adapter-wide discovery and connection establishment.
// The guard is released during GATT reads, allowing readings to overlap.
struct Radio {
    adapter: Adapter,
    ready_at: Mutex<Instant>,
    cache: Mutex<BTreeMap<String, Peripheral>>,
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

        // Try the cached peripheral first—avoids a full scan cycle when the
        // inverter is still advertising from the previous read.
        let cached = self.cache.lock().await.get(&inverter.mac).cloned();
        if let Some(cached) = cached {
            let connect_start = Instant::now();
            let connected = timeout_at(
                deadline.min(Instant::now() + Duration::from_secs(10)),
                cached.connect(),
            )
            .await;
            timing.connect_attempt_ms = connect_start.elapsed().as_millis() as u64;
            match connected {
                Ok(Ok(())) => {
                    info!(
                        connection_ms = timing.connect_attempt_ms,
                        "Bluetooth connected (cached)"
                    );
                    return Ok(cached);
                }
                result => {
                    let error = match result {
                        Ok(Err(error)) => format!("{error:#}"),
                        Err(error) => format!("Cached Bluetooth connection timed out: {error}"),
                        _ => unreachable!(),
                    };
                    warn!(
                        connection_ms = timing.connect_attempt_ms,
                        %error,
                        "Cached Bluetooth connection failed; cleaning up before discovery"
                    );
                    self.cache.lock().await.remove(&inverter.mac);
                    let disconnected = disconnect(&cached, config.disconnect_timeout_seconds).await;
                    info!(disconnected, "Cached connection cleanup finished");
                    *ready_at = Instant::now() + Duration::from_secs(2);
                    timeout_at(deadline, sleep_until(*ready_at))
                        .await
                        .context("Cached connection recovery exceeded polling budget")?;
                }
            }
        }

        debug!(stage = "discovery", "Refreshing Bluetooth discovery");
        let scan_start = Instant::now();
        let discovered = timeout_at(
            deadline.min(Instant::now() + Duration::from_secs(10)),
            async {
                self.adapter.start_scan(ScanFilter::default()).await?;
                sleep(Duration::from_secs(config.scan_duration_seconds)).await;
                self.adapter.peripherals().await
            },
        )
        .await;
        // Always stop discovery, including when its start or enumeration timed out.
        // Keep the coordinator locked until cleanup completes.
        timeout(Duration::from_secs(3), self.adapter.stop_scan())
            .await
            .context("Stop discovery timed out")?
            .context("Stop discovery failed")?;
        timing.scan_ms = scan_start.elapsed().as_millis() as u64;
        let device = discovered
            .context("Discovery timed out")??
            .into_iter()
            .find(|p| p.address().to_string() == inverter.mac)
            .context("Inverter not discovered")?;
        debug!(stage = "connect", "Establishing Bluetooth connection");
        let connect_start = Instant::now();
        let connected = timeout_at(
            deadline.min(Instant::now() + Duration::from_secs(10)),
            device.connect(),
        )
        .await;
        timing.connect_attempt_ms += connect_start.elapsed().as_millis() as u64;
        match connected {
            Ok(Ok(())) => {
                info!(
                    connection_ms = timing.connect_attempt_ms,
                    "Bluetooth connected"
                );
                self.cache
                    .lock()
                    .await
                    .insert(inverter.mac.clone(), device.clone());
                Ok(device)
            }
            result => {
                disconnect(&device, config.disconnect_timeout_seconds).await;
                *ready_at = Instant::now() + Duration::from_secs(2);
                match result {
                    Ok(Err(error)) => Err(error).context("Bluetooth connection failed"),
                    Err(error) => Err(error).context("Bluetooth connection timed out"),
                    _ => unreachable!(),
                }
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

        let (fields, quality) = result??;
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
async fn retry_read<T, F, Fut>(deadline: Instant, mut operation: F) -> Result<T>
where
    F: FnMut(Instant) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut last_error = anyhow::anyhow!("Polling budget expired");
    for attempt in 1..=3 {
        if Instant::now() >= deadline {
            break;
        }
        info!(attempt, "Starting read attempt");
        match operation(deadline).await {
            Ok(value) => return Ok(value),
            Err(error) => {
                warn!(attempt, error = %format!("{error:#}"), "Read attempt failed");
                last_error = error;
            }
        }
        if attempt == 3 {
            break;
        }
        let delay = retry_delay(attempt);
        if Instant::now() + delay >= deadline {
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
        // Sequential mode holds the slot for a complete read/retry cycle;
        // concurrent mode permits one independent worker per inverter.
        let slot = slots.acquire().await?;
        let started = Instant::now();
        // Label the cycle consistently even when a retry connects in a later minute.
        let timestamp = chrono::Utc::now()
            .timestamp_nanos_opt()
            .expect("UTC timestamp fits i64");
        let result = retry_read(started + config.poll_timeout, |deadline| {
            radio.read(config, inverter, deadline)
        })
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
        info!(cycles, successes, seconds_since_success = ?last_success.map(|time| time.elapsed().as_secs()), "Inverter polling status");
        drop(slot);
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
    info!(adapter = %adapter.adapter_info().await?, concurrent = config.concurrent, "Starting collector");
    let radio = Radio {
        adapter: adapter.clone(),
        ready_at: Mutex::new(Instant::now()),
        cache: Mutex::new(BTreeMap::new()),
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
        let deadline = Instant::now() + Duration::from_secs(90);
        let retrying = retry_read(deadline, |_| async {
            let attempt = failures.fetch_add(1, Ordering::SeqCst);
            if attempt < 2 {
                bail!("Simulated connection contention");
            }
            assert_eq!(healthy.load(Ordering::SeqCst), 1);
            Ok(1010)
        });
        let working = retry_read(deadline, |_| async {
            healthy.fetch_add(1, Ordering::SeqCst);
            Ok(1006)
        });
        let (retrying, working) = tokio::join!(retrying, working);
        assert_eq!(retrying.unwrap(), 1010);
        assert_eq!(working.unwrap(), 1006);
        assert_eq!(failures.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn retries_stop_at_attempt_limit_or_budget() {
        for budget in [1, 90] {
            let attempts = AtomicUsize::new(0);
            let started = Instant::now();
            let result: Result<()> = retry_read(started + Duration::from_secs(budget), |_| async {
                attempts.fetch_add(1, Ordering::SeqCst);
                bail!("Unavailable");
            })
            .await;
            assert!(result.is_err());
            assert_eq!(
                attempts.load(Ordering::SeqCst),
                if budget == 1 { 1 } else { 3 }
            );
            assert!(started.elapsed() < Duration::from_secs(budget));
        }
    }

    #[tokio::test]
    async fn concurrent_producers_preserve_distinct_spool_entries() -> Result<()> {
        let spool = env::temp_dir().join(format!("delta-solar-test-{}", Uuid::new_v4()));
        fs::create_dir(&spool).await?;
        let a = "inverter_data,serial=1010 Power=1 123\n";
        let b = "inverter_data,serial=1006 Power=2 123\n";
        tokio::try_join!(enqueue(&spool, a), enqueue(&spool, b))?;
        let mut entries = fs::read_dir(&spool).await?;
        let mut lines = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            assert_eq!(entry.path().extension().unwrap(), "lp");
            lines.push(fs::read_to_string(entry.path()).await?);
        }
        lines.sort();
        assert_eq!(lines, vec![b, a]);
        fs::remove_dir_all(&spool).await?;
        Ok(())
    }

    #[tokio::test]
    async fn upload_failure_retains_point_then_success_removes_it() -> Result<()> {
        let spool = env::temp_dir().join(format!("delta-solar-test-{}", Uuid::new_v4()));
        fs::create_dir(&spool).await?;
        let fields = BTreeMap::from([("Power".into(), 0.0)]);
        let point = line("1010", &fields, 123456789);
        assert_eq!(
            point,
            "inverter_data,model=M4-TL-US,serial=1010 Power=0 123456789\n"
        );
        assert_ne!(point, line("1006", &fields, 123456789));
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
            poll_timeout: Duration::from_secs(90),
            adapter: 0,
            concurrent: true,
            scan_duration_seconds: 2,
            inter_command_sleep_ms: 100,
            disconnect_timeout_seconds: 3,
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
