use std::{
	collections::{HashMap, HashSet},
	path::PathBuf,
	sync::{
		atomic::{AtomicUsize, Ordering},
		Arc, RwLock,
	},
};

use anyhow::Context;
use moq_lite::{Broadcast, BroadcastConsumer, BroadcastProducer, Origin, OriginConsumer, OriginProducer};
use tracing::Instrument;
use url::Url;

use crate::AuthToken;

/// Configuration for relay clustering.
///
/// When a root URL and node name are both set, the relay joins a
/// cluster by connecting to the root and advertising its own hostname.
#[serde_with::serde_as]
#[derive(clap::Args, Clone, Debug, serde::Serialize, serde::Deserialize, Default)]
#[serde_with::skip_serializing_none]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct ClusterConfig {
	/// Connect to this hostname in order to discover other nodes.
	#[serde(alias = "connect")]
	#[arg(
		id = "cluster-root",
		long = "cluster-root",
		env = "MOQ_CLUSTER_ROOT",
		alias = "cluster-connect"
	)]
	pub root: Option<String>,

	/// Use the token in this file when connecting to other nodes.
	#[arg(id = "cluster-token", long = "cluster-token", env = "MOQ_CLUSTER_TOKEN")]
	pub token: Option<PathBuf>,

	/// Our hostname which we advertise to other nodes.
	///
	// TODO Remove alias once we've migrated to the new name.
	#[serde(alias = "advertise")]
	#[arg(
		id = "cluster-node",
		long = "cluster-node",
		env = "MOQ_CLUSTER_NODE",
		alias = "cluster-advertise"
	)]
	pub node: Option<String>,

	/// The prefix to use for cluster announcements.
	/// Defaults to "internal/origins".
	///
	/// WARNING: This should not be accessible by users unless authentication is disabled (YOLO).
	#[arg(
		id = "cluster-prefix",
		long = "cluster-prefix",
		default_value = "internal/origins",
		env = "MOQ_CLUSTER_PREFIX"
	)]
	pub prefix: String,

	/// Maximum subscriber sessions before sending GOAWAY to new connections.
	/// When the relay has this many active subscribers, new subscriber connections
	/// receive a GOAWAY with a redirect URI pointing to a cluster peer.
	/// 0 or unset = unlimited.
	#[arg(
		id = "cluster-max-subscribers",
		long = "cluster-max-subscribers",
		env = "MOQ_CLUSTER_MAX_SUBSCRIBERS"
	)]
	pub max_subscribers: Option<usize>,
}

/// Manages broadcast origins across local and remote relay nodes.
///
/// All broadcasts (local and remote) are stored in a single origin.
/// Hop-based routing ensures the shortest path is preferred and prevents loops.
#[derive(Clone)]
pub struct Cluster {
	pub config: ClusterConfig,
	client: moq_native::Client,

	/// All broadcasts, both local and remote.
	/// Hops-based routing ensures the shortest path is preferred.
	pub origin: OriginProducer,

	/// Number of active non-cluster subscriber sessions.
	pub subscriber_count: Arc<AtomicUsize>,

	/// Known peer hostnames, learned from cluster connections.
	/// Used for GOAWAY redirect target selection.
	pub known_peers: Arc<RwLock<HashSet<String>>>,
}

impl Cluster {
	/// Creates a new cluster with the given configuration and QUIC client.
	pub fn new(config: ClusterConfig, client: moq_native::Client) -> Self {
		Cluster {
			config,
			client,
			origin: Origin::produce(),
			subscriber_count: Arc::new(AtomicUsize::new(0)),
			known_peers: Arc::new(RwLock::new(HashSet::new())),
		}
	}

	/// Returns true if the relay is at or over the subscriber limit.
	pub fn is_over_subscriber_limit(&self) -> bool {
		match self.config.max_subscribers {
			Some(max) if max > 0 => self.subscriber_count.load(Ordering::Relaxed) >= max,
			_ => false,
		}
	}

	/// Pick a redirect target from known cluster peers for GOAWAY.
	/// Returns the HTTPS URL of a peer, or None if no peers are known.
	pub fn pick_redirect_target(&self) -> Option<String> {
		if let Ok(peers) = self.known_peers.read() {
			// Pick the first known peer that isn't us.
			let my_node = self.config.node.as_deref();
			for hostname in peers.iter() {
				if my_node != Some(hostname.as_str()) {
					return Some(format!("https://{hostname}/"));
				}
			}
		}

		// Fallback to cluster root.
		self.config.root.as_ref().map(|h| format!("https://{h}/"))
	}

	/// Create an RAII guard that increments the subscriber count now
	/// and decrements it when dropped.
	pub fn subscriber_guard(&self) -> SubscriberGuard {
		self.subscriber_count.fetch_add(1, Ordering::Relaxed);
		SubscriberGuard {
			count: self.subscriber_count.clone(),
		}
	}

	/// For a given auth token, return the origin consumer for subscribing.
	/// All sessions see the same origin. Hop-counting prevents loops.
	pub fn subscriber(&self, token: &AuthToken) -> Option<OriginConsumer> {
		let origin = self.origin.with_root(&token.root)?;
		origin.consume_only(&token.subscribe)
	}

	/// For a given auth token, return the origin producer for publishing.
	/// All sessions publish to the same origin. Hop-counting prevents loops.
	pub fn publisher(&self, token: &AuthToken) -> Option<OriginProducer> {
		let origin = self.origin.with_root(&token.root)?;
		origin.publish_only(&token.publish)
	}

	/// Register a cluster node's presence.
	///
	/// Returns a [`ClusterRegistration`] that should be kept alive for the duration of the session.
	pub fn register(&self, token: &AuthToken) -> Option<ClusterRegistration> {
		let node = token.register.clone()?;
		let broadcast = Broadcast::produce();

		let path = moq_lite::Path::new(&self.config.prefix).join(&node);
		self.origin.publish_broadcast(path, broadcast.consume());

		// Track this peer for GOAWAY redirect selection.
		if let Ok(mut peers) = self.known_peers.write() {
			if peers.insert(node.clone()) {
				tracing::info!(peer = %node, "registered incoming cluster peer for GOAWAY redirect");
			}
		}

		Some(ClusterRegistration::new(node, broadcast))
	}

	/// Looks up a broadcast by name.
	pub fn get(&self, broadcast: &str) -> Option<BroadcastConsumer> {
		self.origin.consume_broadcast(broadcast)
	}

	/// Runs the cluster event loop, connecting to remote nodes.
	///
	/// This future runs until the cluster is shut down or a fatal error occurs.
	pub async fn run(self) -> anyhow::Result<()> {
		// If we're using a root node, then we have to connect to it.
		// Otherwise, we're the root node so we wait for other nodes to connect to us.
		let Some(root) = self
			.config
			.root
			.clone()
			.filter(|connect| Some(connect) != self.config.node.as_ref())
		else {
			tracing::info!("running as root, accepting leaf nodes");
			// Root node just waits — no outbound connections needed.
			// Leaf nodes connect to us and we discover them via registration broadcasts.
			std::future::pending::<()>().await;
			return Ok(());
		};

		// Subscribe to available origins (what we learn from other nodes).
		// Use with_root to automatically strip the prefix from announced paths.
		let origins = self
			.origin
			.with_root(&self.config.prefix)
			.context("no authorized origins")?;

		// If the token is provided, read it from the disk and use it in the query parameter.
		// TODO put this in an AUTH header once WebTransport supports it.
		let token = match &self.config.token {
			Some(path) => std::fs::read_to_string(path)
				.context("failed to read token")?
				.trim()
				.to_string(),
			None => "".to_string(),
		};

		let local = self.config.node.clone().context("missing node")?;

		// Create a dummy broadcast that we don't close so run_remote doesn't close.
		let noop = Broadcast::produce();

		// Despite returning a Result, we should NEVER return an Ok
		tokio::select! {
			res = self.clone().run_remote(&root, Some(local.as_str()), token.clone(), noop.consume()) => {
				res.context("failed to connect to root")?;
				anyhow::bail!("connection to root closed");
			}
			res = self.clone().run_remotes(origins.consume_only(&[moq_lite::Path::default()]).context("no origins")?, token) => {
				res.context("failed to connect to remotes")?;
				anyhow::bail!("connection to remotes closed");
			}
		}
	}

	async fn run_remotes(self, mut origins: OriginConsumer, token: String) -> anyhow::Result<()> {
		// Cancel tasks when the origin is closed.
		let mut active: HashMap<String, tokio::task::AbortHandle> = HashMap::new();

		// Discover other origins.
		// NOTE: The root node will connect to all other nodes as a client, ignoring the existing (server) connection.
		// This ensures that nodes are advertising a valid hostname before any tracks get announced.
		while let Some((node, origin)) = origins.announced().await {
			if self.config.node.as_deref() == Some(node.as_str()) {
				// Skip ourselves.
				continue;
			}

			let Some(origin) = origin else {
				tracing::info!(%node, "origin cancelled");
				active.remove(node.as_str()).unwrap().abort();
				continue;
			};

			tracing::info!(%node, "discovered origin");

			let this = self.clone();
			let token = token.clone();
			let node2 = node.clone();

			let handle = tokio::spawn(
				async move {
					match this.run_remote(node2.as_str(), None, token, origin).await {
						Ok(()) => tracing::info!(%node2, "origin closed"),
						Err(err) => tracing::warn!(%err, %node2, "origin error"),
					}
				}
				.in_current_span(),
			);

			active.insert(node.to_string(), handle.abort_handle());
		}

		Ok(())
	}

	#[tracing::instrument("remote", skip_all, err, fields(%remote))]
	async fn run_remote(
		mut self,
		remote: &str,
		register: Option<&str>,
		token: String,
		origin: BroadcastConsumer,
	) -> anyhow::Result<()> {
		let mut url = Url::parse(&format!("https://{remote}/"))?;
		{
			let mut q = url.query_pairs_mut();
			if !token.is_empty() {
				q.append_pair("jwt", &token);
			}
			if let Some(register) = register {
				q.append_pair("register", register);
			}
		}
		let mut backoff = 1;

		loop {
			let res = tokio::select! {
				biased;
				_ = origin.closed() => break,
				res = self.run_remote_once(&url) => res,
			};

			match res {
				Ok(()) => backoff = 1,
				Err(err) => {
					backoff *= 2;
					tracing::error!(%err, "remote error");
				}
			}

			let timeout = tokio::time::Duration::from_secs(backoff);
			if timeout > tokio::time::Duration::from_secs(300) {
				// 5 minutes of backoff is enough, just give up.
				anyhow::bail!("remote connection keep failing, giving up");
			}

			tokio::time::sleep(timeout).await;
		}

		Ok(())
	}

	async fn run_remote_once(&mut self, url: &Url) -> anyhow::Result<()> {
		let mut log_url = url.clone();
		log_url.set_query(None);
		tracing::info!(url = %log_url, "connecting to remote");

		// Track this peer hostname for GOAWAY redirect selection.
		if let Some(host) = url.host_str() {
			if let Ok(mut peers) = self.known_peers.write() {
				if peers.insert(host.to_string()) {
					tracing::info!(peer = %host, "registered cluster peer for GOAWAY redirect");
				}
			}
		}

		let session = self
			.client
			.clone()
			.with_publish(self.origin.consume())
			.with_consume(self.origin.clone())
			.connect(url.clone())
			.await
			.context("failed to connect to remote")?;

		session.closed().await.map_err(Into::into)
	}
}

/// A handle that keeps a cluster node registered. Dropping it
/// unregisters the node and aborts its broadcast.
pub struct ClusterRegistration {
	// The name of the node.
	node: String,

	// The announcement, send to other nodes.
	broadcast: BroadcastProducer,
}

impl ClusterRegistration {
	/// Creates a new registration for the given node.
	pub fn new(node: String, broadcast: BroadcastProducer) -> Self {
		tracing::info!(%node, "registered cluster client");
		ClusterRegistration { node, broadcast }
	}
}
impl Drop for ClusterRegistration {
	fn drop(&mut self) {
		tracing::info!(%self.node, "unregistered cluster client");
		let _ = self.broadcast.abort(moq_lite::Error::Cancel);
	}
}

/// RAII guard that decrements the subscriber count when dropped.
pub struct SubscriberGuard {
	count: Arc<AtomicUsize>,
}

impl Drop for SubscriberGuard {
	fn drop(&mut self) {
		self.count.fetch_sub(1, Ordering::Relaxed);
	}
}
