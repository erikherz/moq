//! Pull manager: shared origin-pull logic for directory and warm API.
//!
//! When an edge relay needs content from an origin, PullManager handles the
//! WebTransport connection with reconnect backoff.  Both the Directory (reacting
//! to origin_announced) and the Web warm endpoint use this.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use moq_lite::OriginProducer;
use tokio::sync::Mutex;
use url::Url;

/// Shared pull state that can be used from multiple contexts (Directory, Web API).
#[derive(Clone)]
pub struct PullManager {
	inner: Arc<Mutex<PullState>>,
	directory_root: Option<String>,
}

struct PullState {
	primary: OriginProducer,
	secondary: OriginProducer,
	client: moq_native::Client,
	active_origins: HashMap<String, tokio::task::AbortHandle>,
}

impl PullManager {
	pub fn new(
		primary: OriginProducer,
		secondary: OriginProducer,
		client: moq_native::Client,
		directory_root: Option<String>,
	) -> Self {
		PullManager {
			inner: Arc::new(Mutex::new(PullState {
				primary,
				secondary,
				client,
				active_origins: HashMap::new(),
			})),
			directory_root,
		}
	}

	/// Start pulling content from a remote origin relay.
	/// Returns true if a new pull was started, false if already pulling from this origin.
	pub async fn start_pull(&self, origin_url: &str) -> bool {
		let mut state = self.inner.lock().await;

		if state.active_origins.contains_key(origin_url) {
			return false;
		}

		let url_str = origin_url.to_string();
		let client = state.client.clone();
		let primary = state.primary.clone();
		let secondary = state.secondary.clone();

		tracing::info!(origin = %url_str, "starting content pull from origin");

		let handle = tokio::spawn(async move {
			let mut backoff = 1u64;

			loop {
				let url = match Url::parse(&url_str) {
					Ok(u) => u,
					Err(e) => {
						tracing::error!(%e, origin = %url_str, "invalid origin URL");
						return;
					}
				};

				match client
					.clone()
					.with_publish(primary.consume())
					.with_consume(secondary.clone())
					.connect(url)
					.await
				{
					Ok(session) => {
						backoff = 1;
						tracing::info!(origin = %url_str, "pulling content from origin");
						match session.closed().await {
							Ok(()) => tracing::info!(origin = %url_str, "origin pull connection closed"),
							Err(e) => tracing::warn!(%e, origin = %url_str, "origin pull connection error"),
						}
					}
					Err(e) => {
						tracing::warn!(%e, origin = %url_str, backoff, "failed to connect to origin");
						backoff = (backoff * 2).min(30);
					}
				}

				tokio::time::sleep(Duration::from_secs(backoff)).await;
			}
		});

		state.active_origins.insert(origin_url.to_string(), handle.abort_handle());
		true
	}

	/// Stop pulling content from a remote origin relay.
	pub async fn stop_pull(&self, origin_url: &str) {
		let mut state = self.inner.lock().await;
		if let Some(handle) = state.active_origins.remove(origin_url) {
			tracing::info!(%origin_url, "stopping content pull from origin");
			handle.abort();
		}
	}

	/// Check if we're already pulling from this origin.
	pub async fn is_pulling(&self, origin_url: &str) -> bool {
		let state = self.inner.lock().await;
		state.active_origins.contains_key(origin_url)
	}

	/// Look up a namespace in the Worker directory and start pulling if found.
	/// Returns the origin URL if a pull was started, None if not found or already pulling.
	pub async fn lookup_and_pull(&self, namespace: &str) -> Option<String> {
		let root = self.directory_root.as_deref()?;

		let url = format!("{root}/api/broadcasts/{namespace}");
		tracing::info!(%namespace, %url, "looking up namespace in directory");

		let client = reqwest::Client::builder()
			.timeout(Duration::from_secs(5))
			.build()
			.ok()?;

		let resp = match client.get(&url).send().await {
			Ok(r) => r,
			Err(e) => {
				tracing::warn!(%e, %namespace, "directory lookup failed");
				return None;
			}
		};

		if !resp.status().is_success() {
			tracing::info!(%namespace, status = %resp.status(), "namespace not found in directory");
			return None;
		}

		let body: serde_json::Value = match resp.json().await {
			Ok(v) => v,
			Err(e) => {
				tracing::warn!(%e, %namespace, "directory response parse error");
				return None;
			}
		};

		let origin_url = body["origin_url"].as_str().unwrap_or("");
		if origin_url.is_empty() {
			tracing::info!(%namespace, "no origin_url in directory response");
			return None;
		}

		tracing::info!(%namespace, %origin_url, "directory lookup found origin");

		if self.start_pull(origin_url).await {
			Some(origin_url.to_string())
		} else {
			// Already pulling — still return the URL
			Some(origin_url.to_string())
		}
	}
}
