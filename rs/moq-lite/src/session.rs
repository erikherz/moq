use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use web_transport_trait::Stats;

use crate::{BandwidthConsumer, BandwidthProducer, Error, Version, coding::Writer};

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
	send_bandwidth: Option<BandwidthConsumer>,
	recv_bandwidth: Option<BandwidthConsumer>,
	closed: bool,
}

impl Session {
	pub(super) fn new<S: web_transport_trait::Session>(
		session: S,
		version: Version,
		recv_bandwidth: Option<BandwidthConsumer>,
	) -> Self {
		// Send bandwidth is version-agnostic: it depends on QUIC backend support.
		let send_bandwidth = if session.stats().estimated_send_rate().is_some() {
			let producer = BandwidthProducer::new();
			let consumer = producer.consume();

			let session = session.clone();
			web_async::spawn(async move {
				run_send_bandwidth(&session, producer).await;
			});

			Some(consumer)
		} else {
			None
		};

		Self {
			session: Arc::new(session),
			version,
			send_bandwidth,
			recv_bandwidth,
			closed: false,
		}
	}

	/// Returns the negotiated protocol version.
	pub fn version(&self) -> Version {
		self.version
	}

	/// Returns a consumer for the estimated send bitrate (from the congestion controller).
	///
	/// Returns `None` if the QUIC backend doesn't support bandwidth estimation.
	pub fn send_bandwidth(&self) -> Option<BandwidthConsumer> {
		self.send_bandwidth.clone()
	}

	/// Returns a consumer for the estimated receive bitrate (from PROBE).
	///
	/// Returns `None` if the MoQ version doesn't support PROBE (requires moq-lite-03+).
	pub fn recv_bandwidth(&self) -> Option<BandwidthConsumer> {
		self.recv_bandwidth.clone()
	}

	/// Send a GOAWAY message with a redirect URI, then close the session.
	///
	/// For Lite04+, opens a bidi stream with ControlType::Goaway and sends the Goaway message.
	/// For older versions or IETF, falls back to closing the connection directly.
	pub async fn goaway(&mut self, uri: &str) -> Result<(), Error> {
		tracing::info!(%uri, version = %self.version, "sending GOAWAY");
		self.session.send_goaway(uri, self.version).await?;
		Ok(())
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
	pub async fn closed(&self) -> Result<(), Error> {
		let err = self.session.closed().await;
		Err(Error::Transport(err))
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

/// Polls the QUIC congestion controller for estimated send rate.
///
/// Exits as soon as the session closes so we don't pin the underlying connection
/// after the wrapping [`Session`] is dropped.
async fn run_send_bandwidth<S: web_transport_trait::Session>(session: &S, producer: BandwidthProducer) {
	tokio::select! {
		_ = session.closed() => {}
		_ = producer.closed() => {}
		_ = run_send_bandwidth_inner(session, &producer) => {}
	}
}

/// Toggles between waiting for a consumer and polling stats while one exists.
/// Returns when the producer channel errors (closed by the consumer side).
async fn run_send_bandwidth_inner<S: web_transport_trait::Session>(session: &S, producer: &BandwidthProducer) {
	const POLL_INTERVAL: Duration = Duration::from_millis(100);

	loop {
		if producer.used().await.is_err() {
			return;
		}

		let mut interval = tokio::time::interval(POLL_INTERVAL);
		loop {
			tokio::select! {
				biased;
				res = producer.unused() => {
					if res.is_err() {
						return;
					}
					// No more consumers, pause polling.
					break;
				}
				_ = interval.tick() => {
					let bitrate = session.stats().estimated_send_rate();
					if producer.set(bitrate).is_err() {
						return;
					}
				}
			}
		}
	}
}

// We use a wrapper type that is dyn-compatible to remove the generic bounds from Session.
trait SessionInner: Send + Sync {
	fn close(&self, code: u32, reason: &str);
	fn closed(&self) -> Pin<Box<dyn Future<Output = String> + Send + '_>>;
	fn send_goaway(&self, uri: &str, version: Version) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + '_>>;
	fn stats(&self) -> TransportStats;
}

impl<S: web_transport_trait::Session> SessionInner for S {
	fn close(&self, code: u32, reason: &str) {
		S::close(self, code, reason);
	}

	fn closed(&self) -> Pin<Box<dyn Future<Output = String> + Send + '_>> {
		Box::pin(async move { S::closed(self).await.to_string() })
	}

	fn send_goaway(&self, uri: &str, version: Version) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + '_>> {
		let uri = uri.to_string();
		Box::pin(async move {
			match version {
				Version::Lite(lite_version) => {
					match lite_version {
						crate::lite::Version::Lite01 | crate::lite::Version::Lite02 | crate::lite::Version::Lite03 => {
							// Lite01-03 don't support GOAWAY, just close.
							return Err(Error::Version);
						}
						_ => {}
					}

					// Open a bidi stream and send ControlType::Goaway + Goaway message.
					let (send, _recv) = self.open_bi().await.map_err(Error::from_transport)?;
					let mut writer = Writer::new(send, lite_version);
					writer.encode(&crate::lite::ControlType::Goaway).await?;
					writer.encode(&crate::lite::Goaway { uri: uri.into() }).await?;
					writer.finish()?;

					Ok(())
				}
				Version::Ietf(_) => {
					// IETF GOAWAY requires the SETUP stream which we don't have access to here.
					// For now, return an error — the caller should close the connection instead.
					Err(Error::Version)
				}
			}
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
