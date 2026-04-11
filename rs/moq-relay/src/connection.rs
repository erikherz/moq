use std::time::Duration;

use crate::{Auth, AuthParams, Cluster};

use axum::http;
use moq_native::Request;

/// An incoming connection that has not yet been authenticated.
///
/// Call [`run`](Self::run) to authenticate the request, wire up
/// publish/subscribe origins, and serve the session until it closes.
pub struct Connection {
	/// A numeric identifier for logging.
	pub id: u64,
	/// The raw QUIC/WebTransport request to accept or reject.
	pub request: Request,
	/// The cluster state used to resolve origins.
	pub cluster: Cluster,
	/// The authenticator used to verify credentials.
	pub auth: Auth,
}

impl Connection {
	/// Authenticates and serves this connection until it closes.
	#[tracing::instrument("conn", skip_all, fields(id = self.id))]
	pub async fn run(self) -> anyhow::Result<()> {
		let params = match self.request.url() {
			Some(url) => AuthParams::from_url(url),
			None => AuthParams::default(),
		};

		// Verify the URL before accepting the connection.
		let token = match self.auth.verify(&params).await {
			Ok(token) => token,
			Err(err) => {
				let status: http::StatusCode = err.clone().into();
				let _ = self.request.close(status.as_u16()).await;
				return Err(err.into());
			}
		};

		// Check subscriber limit for non-cluster subscriber sessions.
		let is_subscriber = !token.cluster && !token.subscribe.is_empty();
		let over_limit = is_subscriber && self.cluster.is_over_subscriber_limit();

		if over_limit {
			return self.run_goaway().await;
		}

		// Track subscriber count with RAII guard (decrements on drop).
		let _subscriber_guard = if is_subscriber {
			Some(self.cluster.subscriber_guard())
		} else {
			None
		};

		let publish = self.cluster.publisher(&token);
		let subscribe = self.cluster.subscriber(&token);
		let registration = self.cluster.register(&token);
		let transport = self.request.transport();

		match (&publish, &subscribe) {
			(Some(publish), Some(subscribe)) => {
				tracing::info!(transport, root = %token.root, publish = %publish.allowed().map(|p| p.as_str()).collect::<Vec<_>>().join(","), subscribe = %subscribe.allowed().map(|p| p.as_str()).collect::<Vec<_>>().join(","), "session accepted");
			}
			(Some(publish), None) => {
				tracing::info!(transport, root = %token.root, publish = %publish.allowed().map(|p| p.as_str()).collect::<Vec<_>>().join(","), "publisher accepted");
			}
			(None, Some(subscribe)) => {
				tracing::info!(transport, root = %token.root, subscribe = %subscribe.allowed().map(|p| p.as_str()).collect::<Vec<_>>().join(","), "subscriber accepted")
			}
			_ => anyhow::bail!("invalid session; no allowed paths"),
		}

		// Accept the connection.
		// NOTE: subscribe and publish seem backwards because of how relays work.
		// We publish the tracks the client is allowed to subscribe to.
		// We subscribe to the tracks the client is allowed to publish.
		let session = self
			.request
			.with_publish(subscribe)
			.with_consume(publish)
			// TODO: Uncomment when observability feature is merged
			// .with_stats(stats)
			.ok()
			.await?;

		tracing::info!(version = %session.version(), transport, "negotiated");

		// Wait until the session is closed.
		// Keep registration alive so the cluster node stays announced.
		session.closed().await?;
		drop(registration);
		Ok(())
	}

	/// Accept the session, send GOAWAY with a redirect URI, then close.
	async fn run_goaway(self) -> anyhow::Result<()> {
		let redirect_uri = self.cluster.pick_redirect_target();
		let transport = self.request.transport();

		// Accept the session so we can send GOAWAY on the MoQ layer.
		let mut session = self.request.ok().await?;

		let version = session.version();
		tracing::warn!(
			%version, %transport,
			redirect = redirect_uri.as_deref().unwrap_or("none"),
			subscribers = self.cluster.subscriber_count.load(std::sync::atomic::Ordering::Relaxed),
			max = ?self.cluster.config.max_subscribers,
			"over subscriber limit, sending GOAWAY"
		);

		if let Some(ref uri) = redirect_uri {
			match session.goaway(uri).await {
				Ok(()) => {
					// Give the client time to receive the GOAWAY before closing.
					tokio::time::sleep(Duration::from_secs(2)).await;
				}
				Err(err) => {
					tracing::debug!(%err, "GOAWAY send failed (version may not support it), closing connection");
				}
			}
		}

		session.close(moq_lite::Error::GoAway);
		Ok(())
	}
}
