use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;
use tracing::{info, warn};

/// Relay-wide stats collected via atomic counters.
/// Shared across all connection tasks — increment-only, no locks.
#[derive(Default)]
pub struct RelayStats {
	// Connection counts
	pub connections_active: AtomicU64,
	pub connections_total: AtomicU64,
	pub publishers_active: AtomicU64,
	pub subscribers_active: AtomicU64,

	// Traffic (cumulative bytes)
	pub bytes_ingress: AtomicU64,
	pub bytes_egress: AtomicU64,

	// Error counts
	pub subscribe_errors: AtomicU64,
	pub transport_errors: AtomicU64,
	pub decode_errors: AtomicU64,

	// Subscribe counts
	pub subscribes_active: AtomicU64,
	pub subscribes_total: AtomicU64,
}

impl RelayStats {
	pub fn new() -> Arc<Self> {
		Arc::new(Self::default())
	}

	pub fn conn_opened(&self) {
		self.connections_active.fetch_add(1, Ordering::Relaxed);
		self.connections_total.fetch_add(1, Ordering::Relaxed);
	}

	pub fn conn_closed(&self) {
		self.connections_active.fetch_sub(1, Ordering::Relaxed);
	}

	pub fn publisher_opened(&self) {
		self.publishers_active.fetch_add(1, Ordering::Relaxed);
	}

	pub fn publisher_closed(&self) {
		self.publishers_active.fetch_sub(1, Ordering::Relaxed);
	}

	pub fn subscriber_opened(&self) {
		self.subscribers_active.fetch_add(1, Ordering::Relaxed);
	}

	pub fn subscriber_closed(&self) {
		self.subscribers_active.fetch_sub(1, Ordering::Relaxed);
	}

	pub fn add_ingress(&self, bytes: u64) {
		self.bytes_ingress.fetch_add(bytes, Ordering::Relaxed);
	}

	pub fn add_egress(&self, bytes: u64) {
		self.bytes_egress.fetch_add(bytes, Ordering::Relaxed);
	}

	pub fn subscribe_error(&self) {
		self.subscribe_errors.fetch_add(1, Ordering::Relaxed);
	}

	pub fn transport_error(&self) {
		self.transport_errors.fetch_add(1, Ordering::Relaxed);
	}

	pub fn decode_error(&self) {
		self.decode_errors.fetch_add(1, Ordering::Relaxed);
	}
}

/// Snapshot of relay stats + system metrics, serialized as JSON.
#[derive(Serialize)]
pub struct StatsSnapshot {
	pub relay_id: String,
	pub timestamp: u64,
	pub uptime_seconds: u64,
	pub system: SystemStats,
	pub connections: ConnectionStats,
	pub traffic: TrafficStats,
	pub errors: ErrorStats,
}

#[derive(Serialize)]
pub struct SystemStats {
	pub cpu_percent: f64,
	pub rss_mb: u64,
	pub fd_count: u64,
	pub fd_limit: u64,
}

#[derive(Serialize)]
pub struct ConnectionStats {
	pub active: u64,
	pub total: u64,
	pub publishers: u64,
	pub subscribers: u64,
	pub subscribes_active: u64,
	pub subscribes_total: u64,
}

#[derive(Serialize)]
pub struct TrafficStats {
	pub ingress_bytes: u64,
	pub egress_bytes: u64,
	pub ingress_mbps: f64,
	pub egress_mbps: f64,
}

#[derive(Serialize)]
pub struct ErrorStats {
	pub subscribe_errors: u64,
	pub transport_errors: u64,
	pub decode_errors: u64,
}

/// Read RSS (resident set size) from /proc/self/status
fn read_rss_mb() -> u64 {
	std::fs::read_to_string("/proc/self/status")
		.ok()
		.and_then(|s| {
			s.lines()
				.find(|l| l.starts_with("VmRSS:"))
				.and_then(|l| l.split_whitespace().nth(1))
				.and_then(|v| v.parse::<u64>().ok())
		})
		.map(|kb| kb / 1024)
		.unwrap_or(0)
}

/// Read CPU time from /proc/self/stat (utime + stime in clock ticks)
fn read_cpu_ticks() -> u64 {
	std::fs::read_to_string("/proc/self/stat")
		.ok()
		.and_then(|s| {
			// Fields are space-separated, but comm (field 2) can contain spaces in parens
			let after_comm = s.rfind(')')? ;
			let fields: Vec<&str> = s[after_comm + 2..].split_whitespace().collect();
			// utime is field 14 (index 11 after comm), stime is field 15 (index 12)
			let utime: u64 = fields.get(11)?.parse().ok()?;
			let stime: u64 = fields.get(12)?.parse().ok()?;
			Some(utime + stime)
		})
		.unwrap_or(0)
}

/// Count open file descriptors via /proc/self/fd
fn read_fd_count() -> u64 {
	std::fs::read_dir("/proc/self/fd")
		.map(|d| d.count() as u64)
		.unwrap_or(0)
}

/// Read file descriptor limit
fn read_fd_limit() -> u64 {
	std::fs::read_to_string("/proc/self/limits")
		.ok()
		.and_then(|s| {
			s.lines()
				.find(|l| l.starts_with("Max open files"))
				.and_then(|l| l.split_whitespace().nth(3))
				.and_then(|v| v.parse().ok())
		})
		.unwrap_or(0)
}

/// Background task: collects stats every `interval` and POSTs to `collector_url`.
pub async fn run_stats_reporter(
	relay_id: String,
	stats: Arc<RelayStats>,
	collector_url: String,
	interval: Duration,
) {
	let start = Instant::now();
	let clock_ticks_per_sec = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64;

	let client = reqwest::Client::builder()
		.timeout(Duration::from_secs(5))
		.build()
		.expect("failed to build HTTP client");

	let mut prev_cpu_ticks: u64 = read_cpu_ticks();
	let mut prev_ingress: u64 = stats.bytes_ingress.load(Ordering::Relaxed);
	let mut prev_egress: u64 = stats.bytes_egress.load(Ordering::Relaxed);
	let mut prev_time = Instant::now();

	// Wait one interval before first report
	tokio::time::sleep(interval).await;

	loop {
		let now = Instant::now();
		let dt = now.duration_since(prev_time).as_secs_f64();

		// CPU usage (process, not system)
		let cpu_ticks = read_cpu_ticks();
		let cpu_delta = cpu_ticks.saturating_sub(prev_cpu_ticks) as f64;
		let cpu_percent = if dt > 0.0 {
			(cpu_delta / clock_ticks_per_sec / dt) * 100.0
		} else {
			0.0
		};
		prev_cpu_ticks = cpu_ticks;

		// Traffic deltas
		let ingress = stats.bytes_ingress.load(Ordering::Relaxed);
		let egress = stats.bytes_egress.load(Ordering::Relaxed);
		let ingress_mbps = if dt > 0.0 {
			((ingress - prev_ingress) as f64 * 8.0 / 1e6) / dt
		} else {
			0.0
		};
		let egress_mbps = if dt > 0.0 {
			((egress - prev_egress) as f64 * 8.0 / 1e6) / dt
		} else {
			0.0
		};
		prev_ingress = ingress;
		prev_egress = egress;
		prev_time = now;

		let snapshot = StatsSnapshot {
			relay_id: relay_id.clone(),
			timestamp: std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap_or_default()
				.as_millis() as u64,
			uptime_seconds: start.elapsed().as_secs(),
			system: SystemStats {
				cpu_percent,
				rss_mb: read_rss_mb(),
				fd_count: read_fd_count(),
				fd_limit: read_fd_limit(),
			},
			connections: ConnectionStats {
				active: stats.connections_active.load(Ordering::Relaxed),
				total: stats.connections_total.load(Ordering::Relaxed),
				publishers: stats.publishers_active.load(Ordering::Relaxed),
				subscribers: stats.subscribers_active.load(Ordering::Relaxed),
				subscribes_active: stats.subscribes_active.load(Ordering::Relaxed),
				subscribes_total: stats.subscribes_total.load(Ordering::Relaxed),
			},
			traffic: TrafficStats {
				ingress_bytes: ingress,
				egress_bytes: egress,
				ingress_mbps,
				egress_mbps,
			},
			errors: ErrorStats {
				subscribe_errors: stats.subscribe_errors.load(Ordering::Relaxed),
				transport_errors: stats.transport_errors.load(Ordering::Relaxed),
				decode_errors: stats.decode_errors.load(Ordering::Relaxed),
			},
		};

		// Log locally
		info!(
			relay = %snapshot.relay_id,
			cpu = format!("{:.1}%", snapshot.system.cpu_percent),
			rss = format!("{}MB", snapshot.system.rss_mb),
			fds = snapshot.system.fd_count,
			conns = snapshot.connections.active,
			subs = snapshot.connections.subscribers,
			ingress = format!("{:.1}Mbps", snapshot.traffic.ingress_mbps),
			egress = format!("{:.1}Mbps", snapshot.traffic.egress_mbps),
			"relay stats"
		);

		// POST to collector
		if !collector_url.is_empty() {
			match client.post(&collector_url).json(&snapshot).send().await {
				Ok(resp) if !resp.status().is_success() => {
					warn!(status = %resp.status(), "stats collector rejected payload");
				}
				Err(e) => {
					warn!(%e, "failed to push stats to collector");
				}
				_ => {}
			}
		}

		tokio::time::sleep(interval).await;
	}
}
