use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{Router, routing::put};
use bytes::BytesMut;
use clap::Subcommand;
use hang::moq_lite;
use http_body_util::BodyExt;
use moq_mux::import;

#[derive(Subcommand, Clone)]
pub enum PublishFormat {
	Avc3,
	Fmp4 {
		/// Transmit the fMP4 container directly instead of decoding it.
		#[arg(long)]
		passthrough: bool,
	},
	// NOTE: No aac support because it needs framing.
	Hls {
		/// URL or file path of an HLS playlist to ingest.
		#[arg(long)]
		playlist: String,

		/// Transmit the fMP4 segments directly instead of decoding them.
		#[arg(long)]
		passthrough: bool,
	},
	/// Accept HTTP CMAF-IF PUT/POST from encoders (Ateme, GPAC, FFmpeg).
	Http {
		/// Port to listen on for HTTP ingest.
		#[arg(long, default_value = "9078")]
		port: u16,
	},
}

/// Shared state for the HTTP ingest handler.
/// Holds the Fmp4 decoder and the components needed to recreate it
/// if it gets into a bad state (e.g., stale moof from pre-init errors).
struct HttpState {
	fmp4: import::Fmp4,
	broadcast: moq_lite::BroadcastProducer,
	catalog: moq_mux::CatalogProducer,
}

impl HttpState {
	fn new(broadcast: moq_lite::BroadcastProducer, catalog: moq_mux::CatalogProducer) -> Self {
		let fmp4 = import::Fmp4::new(
			broadcast.clone(),
			catalog.clone(),
			import::Fmp4Config { passthrough: true },
		);
		Self { fmp4, broadcast, catalog }
	}

	/// Reset the decoder to recover from stale state.
	fn reset(&mut self) {
		self.fmp4 = import::Fmp4::new(
			self.broadcast.clone(),
			self.catalog.clone(),
			import::Fmp4Config { passthrough: true },
		);
	}
}

enum PublishDecoder {
	Avc3(Box<import::Avc3>),
	Fmp4(Box<import::Fmp4>),
	Hls(Box<import::Hls>),
	Http {
		state: Arc<Mutex<HttpState>>,
		port: u16,
	},
}

pub struct Publish {
	decoder: PublishDecoder,
	broadcast: moq_lite::BroadcastProducer,
}

impl Publish {
	pub fn new(format: &PublishFormat) -> anyhow::Result<Self> {
		let mut broadcast = moq_lite::BroadcastProducer::default();
		let catalog = moq_mux::CatalogProducer::new(&mut broadcast)?;

		let decoder = match format {
			PublishFormat::Avc3 => {
				let avc3 = import::Avc3::new(broadcast.clone(), catalog.clone());
				PublishDecoder::Avc3(Box::new(avc3))
			}
			PublishFormat::Fmp4 { passthrough } => {
				let fmp4 = import::Fmp4::new(
					broadcast.clone(),
					catalog.clone(),
					import::Fmp4Config {
						passthrough: *passthrough,
					},
				);
				PublishDecoder::Fmp4(Box::new(fmp4))
			}
			PublishFormat::Hls { playlist, passthrough } => {
				let hls = import::Hls::new(
					broadcast.clone(),
					catalog.clone(),
					import::HlsConfig {
						playlist: playlist.clone(),
						client: None,
						passthrough: *passthrough,
					},
				)?;
				PublishDecoder::Hls(Box::new(hls))
			}
			PublishFormat::Http { port } => {
				PublishDecoder::Http {
					state: Arc::new(Mutex::new(HttpState::new(broadcast.clone(), catalog.clone()))),
					port: *port,
				}
			}
		};

		Ok(Self { decoder, broadcast })
	}

	pub fn consume(&self) -> moq_lite::BroadcastConsumer {
		self.broadcast.consume()
	}
}

impl Publish {
	pub async fn run(mut self) -> anyhow::Result<()> {
		match &mut self.decoder {
			PublishDecoder::Avc3(decoder) => {
				let mut stdin = tokio::io::stdin();
				decoder.decode_from(&mut stdin).await
			}
			PublishDecoder::Fmp4(decoder) => {
				let mut stdin = tokio::io::stdin();
				decoder.decode_from(&mut stdin).await
			}
			PublishDecoder::Hls(decoder) => decoder.run().await,
			PublishDecoder::Http { state, port } => {
				run_http_ingest(*port, state.clone()).await
			}
		}
	}
}

/// Run an HTTP server that accepts CMAF-IF PUT/POST from encoders.
/// Each request body is fed into the Fmp4 decoder in passthrough mode.
async fn run_http_ingest(port: u16, state: Arc<Mutex<HttpState>>) -> anyhow::Result<()> {
	let app = Router::new()
		.fallback(put(http_ingest_handler).post(http_ingest_handler))
		.with_state(state);

	let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
	tracing::info!(%addr, "HTTP CMAF-IF ingest listening");

	let listener = tokio::net::TcpListener::bind(addr).await?;
	axum::serve(listener, app).await?;

	Ok(())
}

/// Handle a single HTTP PUT/POST — stream the body into the Fmp4 decoder.
///
/// Each HTTP chunk from the encoder is fed to the decoder as it arrives,
/// so fragments are published with minimal latency (no waiting for the full PUT).
/// Media fragments arriving before the init segment (moov) are dropped silently —
/// the encoder re-sends init segments periodically.
async fn http_ingest_handler(
	State(state): State<Arc<Mutex<HttpState>>>,
	uri: axum::http::Uri,
	request: axum::extract::Request,
) -> impl IntoResponse {
	let path = uri.path().to_string();
	let mut body = request.into_body();
	let mut buf = BytesMut::new();

	tracing::debug!(path, "HTTP ingest stream started");

	while let Some(frame) = body.frame().await {
		let frame = match frame {
			Ok(f) => f,
			Err(e) => {
				tracing::warn!(%e, path, "body read error");
				return StatusCode::BAD_REQUEST;
			}
		};

		let Some(chunk) = frame.data_ref() else {
			continue;
		};

		buf.extend_from_slice(chunk);

		// Try to decode any complete atoms from the buffer.
		let mut http_state = state.lock().unwrap();
		match http_state.fmp4.decode(&mut buf) {
			Ok(()) => {}
			Err(e) => {
				let msg = e.to_string();
				if msg.contains("missing moov") || msg.contains("duplicate moof") || msg.contains("unknown track") || msg.contains("no keyframe") {
					tracing::debug!(path, %e, "skipping fragment");
					buf.clear();
					// Reset the decoder to clear stale internal state (e.g., orphaned moof).
					if !http_state.fmp4.is_initialized() {
						http_state.reset();
					}
				} else {
					tracing::warn!(%e, path, "failed to decode CMAF-IF data");
					return StatusCode::INTERNAL_SERVER_ERROR;
				}
			}
		}
	}

	StatusCode::OK
}
