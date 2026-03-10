use crate::{Auth, AuthParams, Cluster, PullManager};

use axum::http;
use moq_native::Request;

pub struct Connection {
	pub id: u64,
	pub request: Request,
	pub cluster: Cluster,
	pub auth: Auth,
	pub pull: Option<PullManager>,
}

impl Connection {
	#[tracing::instrument("conn", skip_all, fields(id = self.id))]
	pub async fn run(self) -> anyhow::Result<()> {
		let params = match self.request.url() {
			Some(url) => AuthParams::from_url(url),
			None => AuthParams::default(),
		};

		// Verify the URL before accepting the connection.
		let token = match self.auth.verify(&params) {
			Ok(token) => token,
			Err(err) => {
				let status: http::StatusCode = err.clone().into();
				let _ = self.request.close(status.as_u16()).await;
				return Err(err.into());
			}
		};

		// On-demand pull: if this is a non-cluster subscriber and the namespace
		// doesn't exist locally, look it up in the directory and pull from origin.
		let root_str = token.root.as_str();
		if !token.cluster && !root_str.is_empty() {
			if self.cluster.get(root_str).is_none() {
				if let Some(ref pull) = self.pull {
					tracing::info!(namespace = %root_str, "namespace not local, looking up directory");
					if let Some(origin_url) = pull.lookup_and_pull(root_str).await {
						tracing::info!(namespace = %root_str, %origin_url, "pulling from origin, waiting for content");
						// Wait up to 5s for the namespace to appear in the combined origin.
						let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
						while tokio::time::Instant::now() < deadline {
							if self.cluster.get(root_str).is_some() {
								tracing::info!(namespace = %root_str, "namespace available after pull");
								break;
							}
							tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
						}
					}
				}
			}
		}

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
}
