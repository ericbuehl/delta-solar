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
        if interval == 0 || poll_timeout == 0 {
            bail!("Polling durations must be positive");
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
        })
    }
}

async fn collect(device: &Peripheral) -> Result<(BTreeMap<String, f64>, ReadQuality)> {
    debug!(stage = "services", "Discovering GATT services");
    device
        .discover_services()
        .await
        .context("Discover GATT services")?;
    let chars = device.characteristics();
    let write = chars
        .iter()
        .find(|c| c.uuid == WRITE)
        .context("Delta write characteristic missing")?;
    let notify = chars
        .iter()
        .find(|c| c.uuid == NOTIFY)
        .context("Delta notify characteristic missing")?;
    let mut notifications = device.notifications().await?;
    device
        .subscribe(notify)
        .await
        .context("Subscribe to measurements")?;
    debug!(stage = "read", "Reading measurements");
    let mut decoder = protocol::Decoder::default();
    let mut samples: BTreeMap<String, Vec<f64>> = BTreeMap::new();
    let mut quality = ReadQuality::default();
    // Upstream's access command, without replaying its captured clock-setting writes.
    let mut commands = vec![vec![0, 9], vec![120, 5], vec![100, 253, 25, 132]];
    for _ in 0..2 {
        commands.extend(protocol::FIELDS.iter().map(|(_, a, b, _)| vec![*a, *b]));
    }
    for payload in commands {
        quality.commands_requested += 1;
        device
            .write(write, &protocol::command(&payload), WriteType::WithResponse)
            .await?;
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
                    warn!(command = ?payload, "No response to command");
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
        sleep(Duration::from_millis(500)).await;
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
            sleep(Duration::from_millis(500)).await;
        }
    }
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
}

impl Radio {
    async fn connect(&self, inverter: &Inverter, deadline: Instant) -> Result<Peripheral> {
        let mut ready_at = timeout_at(deadline, self.ready_at.lock())
            .await
            .context("Timed out waiting for radio coordinator")?;
        timeout_at(deadline, sleep_until(*ready_at))
            .await
            .context("Radio cooldown exceeded polling budget")?;
        debug!(stage = "discovery", "Refreshing Bluetooth discovery");
        let discovered = timeout_at(
            deadline.min(Instant::now() + Duration::from_secs(10)),
            async {
                self.adapter.start_scan(ScanFilter::default()).await?;
                sleep(Duration::from_secs(5)).await;
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
        let device = discovered
            .context("Discovery timed out")??
            .into_iter()
            .find(|p| p.address().to_string() == inverter.mac)
            .context("Inverter not discovered")?;
        debug!(stage = "connect", "Establishing Bluetooth connection");
        let connected = timeout_at(
            deadline.min(Instant::now() + Duration::from_secs(15)),
            device.connect(),
        )
        .await;
        match connected {
            Ok(Ok(())) => {
                info!("Bluetooth connected");
                Ok(device)
            }
            result => {
                disconnect(&device).await;
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
        inverter: &Inverter,
        deadline: Instant,
    ) -> Result<(i64, BTreeMap<String, f64>, ReadQuality)> {
        let device = self.connect(inverter, deadline).await?;
        let timestamp = chrono::Utc::now()
            .timestamp_nanos_opt()
            .expect("UTC timestamp fits i64");
        let result = timeout_at(deadline, collect(&device))
            .await
            .context("Measurement read timed out");
        // Cleanup is outside the read timeout. Serializing it avoids overlapping
        // radio control operations and enforces a cooldown before the next connect.
        let mut ready_at = self.ready_at.lock().await;
        disconnect(&device).await;
        *ready_at = Instant::now() + Duration::from_secs(2);
        let (fields, quality) = result??;
        Ok((timestamp, fields, quality))
    }
}

async fn disconnect(device: &Peripheral) {
    info!(stage = "disconnect", "Disconnecting Bluetooth");
    match timeout(Duration::from_secs(5), device.disconnect()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!(%error, "Bluetooth disconnect failed"),
        Err(_) => warn!("Bluetooth disconnect timed out"),
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
        let result = retry_read(started + config.poll_timeout, |deadline| {
            radio.read(inverter, deadline)
        })
        .await;
        cycles += 1;
        match result {
            Ok((timestamp, fields, quality)) => {
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
