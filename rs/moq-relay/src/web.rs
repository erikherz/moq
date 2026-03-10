use std::{
	net,
	path::PathBuf,
	pin::Pin,
	sync::{Arc, atomic::AtomicU64},
	task::{Context, Poll, ready},
};

use axum::{
	Router,
	body::Body,
	extract::{Path, Query, State},
	http::{Method, StatusCode},
	response::{IntoResponse, Response},
	routing::get,
};
use bytes::Bytes;
use clap::Parser;
use tower_http::cors::{Any, CorsLayer};

use crate::{Auth, AuthParams, Cluster, PullManager};

use std::collections::HashSet;
use tokio::sync::Mutex;

#[derive(Parser, Clone, Debug, serde::Deserialize, serde::Serialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct WebConfig {
	#[command(flatten)]
	#[serde(default)]
	pub http: HttpConfig,

	#[command(flatten)]
	#[serde(default)]
	pub https: HttpsConfig,

	// If true (default), expose a WebTransport compatible WebSocket polyfill.
	#[arg(long = "web-ws", env = "MOQ_WEB_WS", default_value = "true")]
	#[serde(default = "default_true")]
	pub ws: bool,
}

#[derive(clap::Args, Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct HttpConfig {
	#[arg(long = "web-http-listen", id = "http-listen", env = "MOQ_WEB_HTTP_LISTEN")]
	pub listen: Option<net::SocketAddr>,
}

#[derive(clap::Args, Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct HttpsConfig {
	#[arg(long = "web-https-listen", id = "web-https-listen", env = "MOQ_WEB_HTTPS_LISTEN", requires_all = ["web-https-cert", "web-https-key"])]
	pub listen: Option<net::SocketAddr>,

	/// Load the given certificate from disk.
	#[arg(long = "web-https-cert", id = "web-https-cert", env = "MOQ_WEB_HTTPS_CERT")]
	pub cert: Option<PathBuf>,

	/// Load the given key from disk.
	#[arg(long = "web-https-key", id = "web-https-key", env = "MOQ_WEB_HTTPS_KEY")]
	pub key: Option<PathBuf>,
}

pub struct WebState {
	pub auth: Auth,
	pub cluster: Cluster,
	pub tls_info: Arc<std::sync::RwLock<moq_native::ServerTlsInfo>>,
	pub conn_id: AtomicU64,
	pub pull: Option<PullManager>,
	/// Tracks which namespaces have active warm drain tasks to avoid duplicates.
	pub active_warms: Mutex<HashSet<String>>,
}

// Run a HTTP server using Axum
pub struct Web {
	state: WebState,
	config: WebConfig,
}

impl Web {
	pub fn new(state: WebState, config: WebConfig) -> Self {
		Self { state, config }
	}

	pub async fn run(self) -> anyhow::Result<()> {
		let app = Router::new()
			.route("/certificate.sha256", get(serve_fingerprint))
			.route("/announced", get(serve_announced))
			.route("/announced/{*prefix}", get(serve_announced))
			.route("/fetch/{*path}", get(serve_fetch))
			.route("/api/warm", get(serve_warm));

		// If WebSocket is enabled, add the WebSocket route.
		#[cfg(feature = "websocket")]
		let app = match self.config.ws {
			true => app.route("/{*path}", axum::routing::any(crate::websocket::serve_ws)),
			false => app,
		};

		let app = app
			.layer(CorsLayer::new().allow_origin(Any).allow_methods([Method::GET]))
			.with_state(Arc::new(self.state))
			.into_make_service();

		let http = if let Some(listen) = self.config.http.listen {
			let server = axum_server::bind(listen);
			Some(server.serve(app.clone()))
		} else {
			None
		};

		let https = if let Some(listen) = self.config.https.listen {
			let cert = self.config.https.cert.expect("missing https.cert");
			let key = self.config.https.key.expect("missing https.key");
			let config = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert.clone(), key.clone()).await?;

			#[cfg(unix)]
			tokio::spawn(reload_certs(config.clone(), cert, key));

			let server = axum_server::bind_rustls(listen, config);
			Some(server.serve(app))
		} else {
			None
		};

		tokio::select! {
			Some(res) = async move { Some(http?.await) } => res?,
			Some(res) = async move { Some(https?.await) } => res?,
			else => {},
		};

		Ok(())
	}
}

#[cfg(unix)]
async fn reload_certs(config: axum_server::tls_rustls::RustlsConfig, cert: PathBuf, key: PathBuf) {
	use tokio::signal::unix::{SignalKind, signal};

	// Dunno why we wouldn't be allowed to listen for signals, but just in case.
	let mut listener = signal(SignalKind::user_defined1()).expect("failed to listen for signals");

	while listener.recv().await.is_some() {
		tracing::info!("reloading web certificate");

		if let Err(err) = config.reload_from_pem_file(cert.clone(), key.clone()).await {
			tracing::warn!(%err, "failed to reload web certificate");
		}
	}
}

async fn serve_fingerprint(State(state): State<Arc<WebState>>) -> String {
	// Get the first certificate's fingerprint.
	// TODO serve all of them so we can support multiple signature algorithms.
	state
		.tls_info
		.read()
		.expect("tls_info lock poisoned")
		.fingerprints
		.first()
		.expect("missing certificate")
		.clone()
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct AuthQuery {
	pub(crate) jwt: Option<String>,
	pub(crate) register: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct FetchParams {
	#[serde(flatten)]
	auth: AuthQuery,

	#[serde(default)]
	group: FetchGroup,

	#[serde(default)]
	frame: FetchFrame,
}

#[derive(Debug, Default)]
enum FetchGroup {
	// Return the group at the given sequence number.
	Num(u64),

	// Return the latest group.
	#[default]
	Latest,
}

impl<'de> serde::Deserialize<'de> for FetchGroup {
	fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		let s = String::deserialize(deserializer)?;
		if let Ok(num) = s.parse::<u64>() {
			Ok(FetchGroup::Num(num))
		} else if s == "latest" {
			Ok(FetchGroup::Latest)
		} else {
			Err(serde::de::Error::custom(format!("invalid group value: {s}")))
		}
	}
}

#[derive(Debug, Default)]
enum FetchFrame {
	Num(usize),
	#[default]
	Chunked,
}

impl<'de> serde::Deserialize<'de> for FetchFrame {
	fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		let s = String::deserialize(deserializer)?;
		if let Ok(num) = s.parse::<usize>() {
			Ok(FetchFrame::Num(num))
		} else if s == "chunked" {
			Ok(FetchFrame::Chunked)
		} else {
			Err(serde::de::Error::custom(format!("invalid frame value: {s}")))
		}
	}
}

/// Serve the announced broadcasts for a given prefix.
async fn serve_announced(
	path: Option<Path<String>>,
	Query(query): Query<AuthQuery>,
	State(state): State<Arc<WebState>>,
) -> axum::response::Result<String> {
	let prefix = match path {
		Some(Path(prefix)) => prefix,
		None => String::new(),
	};

	let params = AuthParams {
		path: prefix,
		jwt: query.jwt,
		register: query.register,
	};
	let token = state.auth.verify(&params)?;
	let Some(mut origin) = state.cluster.subscriber(&token) else {
		return Err(StatusCode::UNAUTHORIZED.into());
	};

	let mut broadcasts = Vec::new();

	while let Some((suffix, active)) = origin.try_announced() {
		if active.is_some() {
			broadcasts.push(suffix);
		}
	}

	Ok(broadcasts.iter().map(|p| p.to_string()).collect::<Vec<_>>().join("\n"))
}

/// Serve the given group for a given track
async fn serve_fetch(
	Path(path): Path<String>,
	Query(params): Query<FetchParams>,
	State(state): State<Arc<WebState>>,
) -> axum::response::Result<ServeGroup> {
	// The path containts a broadcast/track
	let mut path: Vec<&str> = path.split("/").collect();
	let track = path.pop().unwrap().to_string();

	// We need at least a broadcast and a track.
	if path.is_empty() {
		return Err(StatusCode::BAD_REQUEST.into());
	}

	let broadcast = path.join("/");
	let auth = AuthParams {
		path: broadcast.clone(),
		jwt: params.auth.jwt,
		register: params.auth.register,
	};
	let token = state.auth.verify(&auth)?;

	let Some(origin) = state.cluster.subscriber(&token) else {
		return Err(StatusCode::UNAUTHORIZED.into());
	};

	tracing::info!(%broadcast, %track, "fetching track");

	let track = moq_lite::Track {
		name: track,
		priority: 0,
	};

	// NOTE: The auth token is already scoped to the broadcast.
	let broadcast = origin.consume_broadcast("").ok_or(StatusCode::NOT_FOUND)?;
	let mut track = broadcast.subscribe_track(&track).map_err(|err| match err {
		moq_lite::Error::NotFound => StatusCode::NOT_FOUND,
		_ => StatusCode::INTERNAL_SERVER_ERROR,
	})?;

	let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(30);

	let result = tokio::time::timeout_at(deadline, async {
		let group = match params.group {
			FetchGroup::Latest => track.next_group().await,
			FetchGroup::Num(sequence) => track.get_group(sequence).await,
		};

		let group = match group {
			Ok(Some(group)) => group,
			Ok(None) => return Err(StatusCode::NOT_FOUND),
			Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
		};

		tracing::info!(track = %track.info.name, group = %group.info.sequence, "serving group");

		match params.frame {
			FetchFrame::Num(index) => match group.get_frame(index).await {
				Ok(Some(frame)) => Ok(ServeGroup {
					group: None,
					frame: Some(frame),
					deadline,
				}),
				Ok(None) => Err(StatusCode::NOT_FOUND),
				Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
			},
			FetchFrame::Chunked => Ok(ServeGroup {
				group: Some(group),
				frame: None,
				deadline,
			}),
		}
	})
	.await;

	match result {
		Ok(Ok(serve)) => Ok(serve),
		Ok(Err(status)) => Err(status.into()),
		Err(_) => Err(StatusCode::GATEWAY_TIMEOUT.into()),
	}
}

struct ServeGroup {
	group: Option<moq_lite::GroupConsumer>,
	frame: Option<moq_lite::FrameConsumer>,
	deadline: tokio::time::Instant,
}

impl ServeGroup {
	async fn next(&mut self) -> moq_lite::Result<Option<Bytes>> {
		while self.group.is_some() || self.frame.is_some() {
			if let Some(frame) = self.frame.as_mut() {
				let data = tokio::time::timeout_at(self.deadline, frame.read_all())
					.await
					.map_err(|_| moq_lite::Error::Timeout)?;
				self.frame.take();
				return Ok(Some(data?));
			}

			if let Some(group) = self.group.as_mut() {
				self.frame = tokio::time::timeout_at(self.deadline, group.next_frame())
					.await
					.map_err(|_| moq_lite::Error::Timeout)??;
				if self.frame.is_none() {
					self.group.take();
				}
			}
		}

		Ok(None)
	}
}

impl IntoResponse for ServeGroup {
	fn into_response(self) -> Response {
		Response::new(Body::new(self))
	}
}

impl http_body::Body for ServeGroup {
	type Data = Bytes;
	type Error = ServeGroupError;

	fn poll_frame(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
	) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
		let this = self.get_mut();

		// Use `poll_fn` to turn the async function into a Future
		let future = this.next();
		tokio::pin!(future);

		match ready!(future.poll(cx)) {
			Ok(Some(data)) => {
				let frame = http_body::Frame::data(data);
				Poll::Ready(Some(Ok(frame)))
			}
			Ok(None) => Poll::Ready(None),
			Err(e) => Poll::Ready(Some(Err(ServeGroupError(e)))),
		}
	}
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
struct ServeGroupError(moq_lite::Error);

impl IntoResponse for ServeGroupError {
	fn into_response(self) -> Response {
		(StatusCode::INTERNAL_SERVER_ERROR, self.0.to_string()).into_response()
	}
}

#[derive(Debug, serde::Deserialize)]
struct WarmQuery {
	namespace: String,
}

/// Warm up this relay for a given namespace.
///
/// Two modes:
/// - **Origin (local)**: namespace exists locally (publisher connected).  Spawn
///   an internal drain subscriber that subscribes to catalog + media tracks,
///   forcing media to flow from the publisher into the relay's cache *before*
///   any real viewer connects.
/// - **Edge (remote)**: namespace doesn't exist locally.  Look up the origin in
///   the Worker directory and start a pull connection.
async fn serve_warm(
	Query(query): Query<WarmQuery>,
	State(state): State<Arc<WebState>>,
) -> axum::response::Result<Response> {
	// Check if the namespace exists locally (origin relay case).
	// Poll briefly — the ANNOUNCE from moqcdn-publish may still be in flight.
	let local_broadcast = {
		let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
		loop {
			if let Some(b) = state.cluster.get(&query.namespace) {
				break Some(b);
			}
			if tokio::time::Instant::now() >= deadline {
				break None;
			}
			tokio::time::sleep(std::time::Duration::from_millis(100)).await;
		}
	};

	if let Some(broadcast) = local_broadcast {
		// Check if we already have a drain running for this namespace
		{
			let warms = state.active_warms.lock().await;
			if warms.contains(&query.namespace) {
				return Ok((StatusCode::OK, serde_json::json!({
					"status": "ready",
					"namespace": query.namespace,
					"warm": true,
				}).to_string()).into_response());
			}
		}

		// Spawn an internal drain subscriber to force media flow
		let ns = query.namespace.clone();
		let active_warms = state.active_warms.lock().await;
		// Double-check after acquiring lock
		if active_warms.contains(&ns) {
			return Ok((StatusCode::OK, serde_json::json!({
				"status": "ready",
				"namespace": ns,
				"warm": true,
			}).to_string()).into_response());
		}
		drop(active_warms);

		let ns_clone = ns.clone();
		let state_clone = state.clone();
		tokio::spawn(async move {
			state_clone.active_warms.lock().await.insert(ns_clone.clone());
			tracing::info!(namespace = %ns_clone, "starting warm drain subscriber");

			if let Err(e) = run_warm_drain(broadcast, &ns_clone).await {
				tracing::warn!(%e, namespace = %ns_clone, "warm drain ended");
			}

			state_clone.active_warms.lock().await.remove(&ns_clone);
			tracing::info!(namespace = %ns_clone, "warm drain stopped");
		});

		return Ok((StatusCode::OK, serde_json::json!({
			"status": "warming",
			"namespace": ns,
		}).to_string()).into_response());
	}

	// Edge relay case: look up origin in the Worker directory and start pulling
	let Some(ref pull) = state.pull else {
		return Ok((StatusCode::NOT_FOUND, serde_json::json!({
			"status": "not_found",
			"namespace": query.namespace,
		}).to_string()).into_response());
	};

	match pull.lookup_and_pull(&query.namespace).await {
		Some(origin_url) => {
			// Wait for the namespace to appear in the combined origin (up to 5s).
			let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
			let mut ready = false;
			while tokio::time::Instant::now() < deadline {
				if state.cluster.get(&query.namespace).is_some() {
					ready = true;
					break;
				}
				tokio::time::sleep(std::time::Duration::from_millis(100)).await;
			}

			let status = if ready { "ready" } else { "pulling" };
			tracing::info!(namespace = %query.namespace, %status, %origin_url, "warm complete");

			Ok((StatusCode::OK, serde_json::json!({
				"status": status,
				"namespace": query.namespace,
				"origin_url": origin_url,
			}).to_string()).into_response())
		}
		None => {
			Ok((StatusCode::NOT_FOUND, serde_json::json!({
				"status": "not_found",
				"namespace": query.namespace,
			}).to_string()).into_response())
		}
	}
}

/// Subscribe to catalog + media tracks on a local broadcast and drain all data.
/// This forces the relay to send SUBSCRIBE to the publisher, causing media to
/// flow into the relay's group cache before any real viewer connects.
async fn run_warm_drain(broadcast: moq_lite::BroadcastConsumer, namespace: &str) -> anyhow::Result<()> {
	// Subscribe to catalog track
	let catalog_track = moq_lite::Track { name: "catalog".to_string(), priority: 0 };
	let mut catalog = broadcast.subscribe_track(&catalog_track)
		.map_err(|e| anyhow::anyhow!("failed to subscribe to catalog: {e}"))?;

	tracing::info!(%namespace, "warm: subscribed to catalog");

	// Read first catalog group to discover media track names
	let track_names: Vec<String> = loop {
		match catalog.next_group().await {
			Ok(Some(mut group)) => {
				if let Ok(Some(mut frame)) = group.next_frame().await {
					let data = frame.read_all().await
						.map_err(|e| anyhow::anyhow!("failed to read catalog frame: {e}"))?;
					let json: serde_json::Value = serde_json::from_slice(&data)?;
					let names: Vec<String> = json["tracks"]
						.as_array()
						.unwrap_or(&vec![])
						.iter()
						.filter(|t| {
							let pkg = t["packaging"].as_str().unwrap_or("");
							pkg == "cmaf" || pkg == "loc"
						})
						.filter_map(|t| t["name"].as_str().map(String::from))
						.collect();
					tracing::info!(%namespace, ?names, "warm: discovered {} media tracks", names.len());
					break names;
				}
			}
			Ok(None) => return Err(anyhow::anyhow!("catalog track closed without data")),
			Err(e) => return Err(anyhow::anyhow!("catalog error: {e}")),
		}
	};

	// Subscribe to each media track and spawn drain tasks
	let mut handles = Vec::new();
	for name in &track_names {
		let track = moq_lite::Track { name: name.clone(), priority: 0 };
		match broadcast.subscribe_track(&track) {
			Ok(mut consumer) => {
				tracing::info!(%namespace, track = %name, "warm: subscribed to track");
				let ns = namespace.to_string();
				let tn = name.clone();
				handles.push(tokio::spawn(async move {
					loop {
						match consumer.next_group().await {
							Ok(Some(mut group)) => {
								while let Ok(Some(mut frame)) = group.next_frame().await {
									// Read and discard — the point is to trigger media flow
									let _ = frame.read_all().await;
								}
							}
							Ok(None) => {
								tracing::info!(namespace = %ns, track = %tn, "warm: track closed");
								break;
							}
							Err(e) => {
								tracing::warn!(%e, namespace = %ns, track = %tn, "warm: track error");
								break;
							}
						}
					}
				}));
			}
			Err(e) => {
				tracing::warn!(%e, %namespace, track = %name, "warm: failed to subscribe to track");
			}
		}
	}

	// Also keep draining catalog updates
	let ns = namespace.to_string();
	handles.push(tokio::spawn(async move {
		loop {
			match catalog.next_group().await {
				Ok(Some(mut group)) => {
					while let Ok(Some(mut frame)) = group.next_frame().await {
						let _ = frame.read_all().await;
					}
				}
				Ok(None) | Err(_) => break,
			}
		}
		tracing::info!(namespace = %ns, "warm: catalog drain ended");
	}));

	// Wait for all drain tasks (runs until publisher disconnects)
	futures::future::join_all(handles).await;
	Ok(())
}

fn default_true() -> bool {
	true
}
