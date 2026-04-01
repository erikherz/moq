use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use crate::{Error, Version};

/// Transport-level connection statistics.
#[derive(Debug, Clone, Default)]
pub struct TransportStats {
	pub bytes_sent: Option<u64>,
	pub bytes_received: Option<u64>,
	pub bytes_lost: Option<u64>,
	pub packets_sent: Option<u64>,
	pub packets_received: Option<u64>,
	pub packets_lost: Option<u64>,
	pub rtt: Option<Duration>,
	pub estimated_send_rate: Option<u64>,
}

/// A MoQ transport session, wrapping a WebTransport connection.
///
/// Created via:
/// - [`crate::Client::connect`] for clients.
/// - [`crate::Server::accept`] for servers.
#[derive(Clone)]
pub struct Session {
	session: Arc<dyn SessionInner>,
	version: Version,
	closed: bool,
}

impl Session {
	pub(super) fn new<S: web_transport_trait::Session>(session: S, version: Version) -> Self {
		Self {
			session: Arc::new(session),
			version,
			closed: false,
		}
	}

	/// Returns the negotiated protocol version.
	pub fn version(&self) -> Version {
		self.version
	}

	/// Close the underlying transport session.
	pub fn close(&mut self, err: Error) {
		if self.closed {
			return;
		}
		self.closed = true;
		self.session.close(err.to_code(), err.to_string().as_ref());
	}

	/// Block until the transport session is closed.
	// TODO Remove the Result the next time we make a breaking change.
	pub async fn closed(&self) -> Result<(), Error> {
		self.session.closed().await;
		Err(Error::Transport)
	}

	/// Get transport-level connection statistics.
	pub fn stats(&self) -> TransportStats {
		self.session.stats()
	}
}

impl Drop for Session {
	fn drop(&mut self) {
		if !self.closed {
			self.session.close(Error::Cancel.to_code(), "dropped");
		}
	}
}

// We use a wrapper type that is dyn-compatible to remove the generic bounds from Session.
trait SessionInner: Send + Sync {
	fn close(&self, code: u32, reason: &str);
	fn closed(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
	fn stats(&self) -> TransportStats;
}

impl<S: web_transport_trait::Session> SessionInner for S {
	fn close(&self, code: u32, reason: &str) {
		S::close(self, code, reason);
	}

	fn closed(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
		Box::pin(async move {
			let _ = S::closed(self).await;
		})
	}

	fn stats(&self) -> TransportStats {
		let s = S::stats(self);
		TransportStats {
			bytes_sent: web_transport_trait::Stats::bytes_sent(&s),
			bytes_received: web_transport_trait::Stats::bytes_received(&s),
			bytes_lost: web_transport_trait::Stats::bytes_lost(&s),
			packets_sent: web_transport_trait::Stats::packets_sent(&s),
			packets_received: web_transport_trait::Stats::packets_received(&s),
			packets_lost: web_transport_trait::Stats::packets_lost(&s),
			rtt: web_transport_trait::Stats::rtt(&s),
			estimated_send_rate: web_transport_trait::Stats::estimated_send_rate(&s),
		}
	}
}
