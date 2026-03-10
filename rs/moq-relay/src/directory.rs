use std::time::Duration;

use futures::{SinkExt, StreamExt};
use moq_lite::OriginProducer;
use tokio_tungstenite::tungstenite::Message;

use crate::PullManager;

#[serde_with::serde_as]
#[derive(clap::Args, Clone, Debug, serde::Serialize, serde::Deserialize, Default)]
#[serde_with::skip_serializing_none]
#[serde(default, deny_unknown_fields)]
pub struct DirectoryConfig {
	/// Cloudflare Worker URL for directory API (e.g. https://moqcdn.net)
	#[arg(
		id = "directory-root",
		long = "directory-root",
		env = "MOQ_DIRECTORY_ROOT"
	)]
	pub root: Option<String>,

	/// This node's public hostname (e.g. usc.l3dcmaf.com)
	#[arg(
		id = "directory-node",
		long = "directory-node",
		env = "MOQ_DIRECTORY_NODE"
	)]
	pub node: Option<String>,

	/// Region label for this node (e.g. us-central)
	#[arg(
		id = "directory-region",
		long = "directory-region",
		env = "MOQ_DIRECTORY_REGION"
	)]
	pub region: Option<String>,
}

impl DirectoryConfig {
	pub fn is_enabled(&self) -> bool {
		self.root.is_some() && self.node.is_some()
	}

	/// Build the WebSocket URL from the HTTP root URL.
	fn ws_url(&self) -> String {
		let root = self.root.as_deref().unwrap_or("");
		let ws_root = root
			.replace("https://", "wss://")
			.replace("http://", "ws://");
		format!("{ws_root}/api/directory/ws")
	}
}

pub struct Directory {
	config: DirectoryConfig,
	primary: OriginProducer,
	pull: PullManager,
}

impl Directory {
	pub fn new(
		config: DirectoryConfig,
		primary: OriginProducer,
		pull: PullManager,
	) -> Self {
		Directory {
			config,
			primary,
			pull,
		}
	}

	/// Main loop: connect WebSocket, watch for announce/unannounce, reconnect on failure.
	pub async fn run(mut self) -> anyhow::Result<()> {
		let mut backoff = 1u64;

		loop {
			match self.run_once().await {
				Ok(()) => {
					backoff = 1;
					tracing::info!("directory connection closed cleanly, reconnecting");
				}
				Err(e) => {
					tracing::warn!(%e, backoff, "directory connection failed, retrying");
					backoff = (backoff * 2).min(30);
				}
			}

			tokio::time::sleep(Duration::from_secs(backoff)).await;
		}
	}

	/// Single connection attempt: connect, register, then relay events.
	async fn run_once(&mut self) -> anyhow::Result<()> {
		let ws_url = self.config.ws_url();
		tracing::info!(%ws_url, "connecting to directory");

		let (ws_stream, _response) = tokio_tungstenite::connect_async(&ws_url).await?;
		let (mut sink, mut stream) = ws_stream.split();

		// Register this node
		let node = self.config.node.as_deref().unwrap_or("");
		let register_msg = serde_json::json!({
			"type": "register",
			"node": node,
			"url": format!("https://{node}"),
			"protocol": "moq-lite-03",
			"region": self.config.region,
		});
		sink.send(Message::Text(register_msg.to_string())).await?;

		// Wait for registration confirmation
		if let Some(msg) = stream.next().await {
			let msg = msg?;
			if let Message::Text(text) = msg {
				tracing::info!(response = %text, "directory register response");
			}
		}

		tracing::info!(%node, "registered with directory via WebSocket");

		let mut consumer = self.primary.consume();
		let mut ping_interval = tokio::time::interval(Duration::from_secs(30));

		loop {
			tokio::select! {
				// Watch for announce/unannounce from the primary origin
				announced = consumer.announced() => {
					match announced {
						Some((path, Some(_broadcast))) => {
							let ns = path.to_string();
							let msg = serde_json::json!({
								"type": "announce",
								"namespace": ns,
							});
							sink.send(Message::Text(msg.to_string())).await?;
							tracing::info!(%ns, "announced to directory");
						}
						Some((path, None)) => {
							let ns = path.to_string();
							let msg = serde_json::json!({
								"type": "unannounce",
								"namespace": ns,
							});
							sink.send(Message::Text(msg.to_string())).await?;
							tracing::info!(%ns, "unannounced from directory");
						}
						None => {
							tracing::info!("primary origin closed");
							break;
						}
					}
				}

				// Read messages from the Worker (acks, announcements from other nodes, etc.)
				msg = stream.next() => {
					match msg {
						Some(Ok(Message::Text(text))) => {
							self.handle_message(&text).await;
						}
						Some(Ok(Message::Close(_))) | None => {
							tracing::info!("directory WebSocket closed by server");
							break;
						}
						Some(Ok(_)) => {} // ignore binary, ping, pong
						Some(Err(e)) => {
							tracing::warn!(%e, "directory WebSocket error");
							break;
						}
					}
				}

				// Keepalive ping
				_ = ping_interval.tick() => {
					let msg = serde_json::json!({ "type": "ping" });
					sink.send(Message::Text(msg.to_string())).await?;
				}
			}
		}

		Ok(())
	}

	/// Handle messages from the directory Worker.
	async fn handle_message(&mut self, text: &str) {
		let data: serde_json::Value = match serde_json::from_str(text) {
			Ok(v) => v,
			Err(_) => return,
		};

		match data.get("type").and_then(|t| t.as_str()) {
			Some("origin_announced") => {
				let origin_url = data["origin_url"].as_str().unwrap_or("");
				let namespace = data["namespace"].as_str().unwrap_or("");
				let origin_node = data["node"].as_str().unwrap_or("");

				if origin_url.is_empty() {
					return;
				}

				tracing::info!(%namespace, %origin_node, %origin_url, "origin announced via directory");

				// Start a pull connection to this origin (one connection per origin, not per namespace)
				self.pull.start_pull(origin_url).await;
			}
			Some("origin_disconnected") => {
				let origin_url = data["origin_url"].as_str().unwrap_or("");
				let origin_node = data["node"].as_str().unwrap_or("");

				tracing::info!(%origin_node, %origin_url, "origin disconnected from directory");

				if !origin_url.is_empty() {
					self.pull.stop_pull(origin_url).await;
				}
			}
			_ => {
				tracing::debug!(msg = %text, "directory message");
			}
		}
	}
}
