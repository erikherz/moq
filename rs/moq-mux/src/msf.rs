//! MSF catalog conversion and helpers.
//!
//! Converts a [`hang::Catalog`] to an MSF [`moq_msf::Catalog`].

use base64::Engine;

/// Convert a hang catalog to an MSF catalog.
pub fn to_msf(catalog: &hang::Catalog) -> moq_msf::Catalog {
	to_msf_with_namespace(catalog, None)
}

/// Convert a hang catalog to an MSF catalog with a namespace set on each track.
pub fn to_msf_with_namespace(catalog: &hang::Catalog, namespace: Option<&str>) -> moq_msf::Catalog {
	let mut tracks = Vec::new();

	let has_multiple_video = catalog.video.renditions.len() > 1;
	for (name, config) in &catalog.video.renditions {
		let packaging = match &config.container {
			hang::catalog::Container::Cmaf { .. } => moq_msf::Packaging::Cmaf,
			_ => moq_msf::Packaging::Legacy,
		};

		let init_data = match &config.container {
			hang::catalog::Container::Cmaf { init_data } => Some(init_data.clone()),
			_ => config
				.description
				.as_ref()
				.map(|d| base64::engine::general_purpose::STANDARD.encode(d.as_ref())),
		};

		let is_cmaf = matches!(packaging, moq_msf::Packaging::Cmaf);
		tracks.push(moq_msf::Track {
			name: name.clone(),
			namespace: namespace.map(|s| s.to_string()),
			packaging,
			is_live: true,
			role: Some(moq_msf::Role::Video),
			codec: Some(config.codec.to_string()),
			width: config.coded_width,
			height: config.coded_height,
			framerate: config.framerate,
			samplerate: None,
			channel_config: None,
			bitrate: config.bitrate,
			init_data,
			render_group: Some(1),
			alt_group: if has_multiple_video { Some(1) } else { None },
			// CMSF: SAP type 1 = IDR (closed GOP) for H.264/H.265 group starts
			max_grp_sap_starting_type: if is_cmaf { Some(1) } else { None },
			max_obj_sap_starting_type: if is_cmaf { Some(1) } else { None },
			event_type: None,
			target_latency: None,
		});
	}

	let has_multiple_audio = catalog.audio.renditions.len() > 1;
	for (name, config) in &catalog.audio.renditions {
		let packaging = match &config.container {
			hang::catalog::Container::Cmaf { .. } => moq_msf::Packaging::Cmaf,
			_ => moq_msf::Packaging::Legacy,
		};

		let init_data = match &config.container {
			hang::catalog::Container::Cmaf { init_data } => Some(init_data.clone()),
			_ => config
				.description
				.as_ref()
				.map(|d| base64::engine::general_purpose::STANDARD.encode(d.as_ref())),
		};

		tracks.push(moq_msf::Track {
			name: name.clone(),
			namespace: namespace.map(|s| s.to_string()),
			packaging,
			is_live: true,
			role: Some(moq_msf::Role::Audio),
			codec: Some(config.codec.to_string()),
			width: None,
			height: None,
			framerate: None,
			samplerate: Some(config.sample_rate),
			channel_config: Some(config.channel_count.to_string()),
			bitrate: config.bitrate,
			init_data,
			render_group: Some(1),
			alt_group: if has_multiple_audio { Some(1) } else { None },
			max_grp_sap_starting_type: None,
			max_obj_sap_starting_type: None,
			event_type: None,
			target_latency: None,
		});
	}

	let generated_at = {
		use std::time::SystemTime;
		let now = SystemTime::now()
			.duration_since(SystemTime::UNIX_EPOCH)
			.unwrap_or_default()
			.as_millis();
		// ISO 8601 UTC: approximate from epoch millis
		let secs = (now / 1000) as i64;
		let ms = (now % 1000) as u32;
		let (y, mo, d, h, mi, s) = epoch_to_utc(secs);
		Some(format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{ms:03}Z"))
	};

	moq_msf::Catalog {
		version: 1,
		generated_at,
		tracks,
	}
}

/// CMSF SAP event type identifier.
pub const CMSF_SAP_EVENT_TYPE: &str = "org.ietf.moq.cmsf.sap";

/// Create an MSF Track entry for a CMSF SAP-type event timeline.
pub fn sap_timeline_track(name: &str, namespace: Option<&str>) -> moq_msf::Track {
	moq_msf::Track {
		name: name.to_string(),
		namespace: namespace.map(|s| s.to_string()),
		packaging: moq_msf::Packaging::EventTimeline,
		is_live: true,
		role: None,
		codec: None,
		width: None,
		height: None,
		framerate: None,
		samplerate: None,
		channel_config: None,
		bitrate: None,
		init_data: None,
		render_group: None,
		alt_group: None,
		max_grp_sap_starting_type: None,
		max_obj_sap_starting_type: None,
		event_type: Some(CMSF_SAP_EVENT_TYPE.to_string()),
		target_latency: None,
	}
}

/// Format a CMSF SAP event JSON payload.
///
/// `sap_type`: 0 = no SAP, 1 = IDR (closed GOP), 2 = open GOP (CRA), 3 = gradual decoding refresh.
/// `ept_ms`: earliest presentation timestamp in milliseconds.
pub fn sap_event_json(sap_type: u32, ept_ms: u64) -> String {
	format!("{{\"l\":[{},{}]}}", sap_type, ept_ms)
}

/// Convert Unix epoch seconds to (year, month, day, hour, minute, second) UTC.
fn epoch_to_utc(epoch: i64) -> (i64, u32, u32, u32, u32, u32) {
	let secs_per_day: i64 = 86400;
	let mut days = epoch / secs_per_day;
	let day_secs = (epoch % secs_per_day) as u32;
	let h = day_secs / 3600;
	let mi = (day_secs % 3600) / 60;
	let s = day_secs % 60;

	// Days since 1970-01-01
	days += 719468; // shift to 0000-03-01 epoch
	let era = days / 146097;
	let doe = days - era * 146097;
	let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
	let y = yoe + era * 400;
	let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
	let mp = (5 * doy + 2) / 153;
	let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
	let mo = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
	let y = if mo <= 2 { y + 1 } else { y };
	(y, mo, d, h, mi, s)
}

/// Publish the MSF catalog derived from a hang catalog to the given track.
pub fn publish(catalog: &hang::Catalog, track: &mut moq_lite::TrackProducer) {
	let msf = to_msf(catalog);
	let Ok(mut group) = track.append_group() else {
		return;
	};
	let _ = group.write_frame(msf.to_string().expect("invalid MSF catalog"));
	let _ = group.finish();
}

#[cfg(test)]
mod test {
	use std::collections::BTreeMap;

	use bytes::Bytes;
	use hang::catalog::{Audio, AudioCodec, AudioConfig, Container, H264, Video, VideoConfig};

	use super::*;

	#[test]
	fn convert_simple() {
		let mut video_renditions = BTreeMap::new();
		video_renditions.insert(
			"video0.avc3".to_string(),
			VideoConfig {
				codec: H264 {
					profile: 0x64,
					constraints: 0x00,
					level: 0x1f,
					inline: true,
				}
				.into(),
				description: None,
				coded_width: Some(1280),
				coded_height: Some(720),
				display_ratio_width: None,
				display_ratio_height: None,
				bitrate: Some(6_000_000),
				framerate: Some(30.0),
				optimize_for_latency: None,
				container: Container::Legacy,
				jitter: None,
			},
		);

		let mut audio_renditions = BTreeMap::new();
		audio_renditions.insert(
			"audio0".to_string(),
			AudioConfig {
				codec: AudioCodec::Opus,
				sample_rate: 48_000,
				channel_count: 2,
				bitrate: Some(128_000),
				description: None,
				container: Container::Legacy,
				jitter: None,
			},
		);

		let catalog = hang::Catalog {
			video: Video {
				renditions: video_renditions,
				display: None,
				rotation: None,
				flip: None,
			},
			audio: Audio {
				renditions: audio_renditions,
			},
			..Default::default()
		};

		let msf = to_msf(&catalog);

		assert_eq!(msf.version, 1);
		assert!(msf.generated_at.is_some());
		assert_eq!(msf.tracks.len(), 2);

		let video = &msf.tracks[0];
		assert_eq!(video.name, "video0.avc3");
		assert_eq!(video.namespace, None);
		assert_eq!(video.role, Some(moq_msf::Role::Video));
		assert_eq!(video.packaging, moq_msf::Packaging::Legacy);
		// Legacy packaging doesn't get CMSF SAP fields
		assert_eq!(video.max_grp_sap_starting_type, None);
		assert_eq!(video.codec, Some("avc3.64001f".to_string()));
		assert_eq!(video.width, Some(1280));
		assert_eq!(video.height, Some(720));
		assert_eq!(video.framerate, Some(30.0));
		assert_eq!(video.bitrate, Some(6_000_000));
		assert!(video.init_data.is_none());

		let audio = &msf.tracks[1];
		assert_eq!(audio.name, "audio0");
		assert_eq!(audio.role, Some(moq_msf::Role::Audio));
		assert_eq!(audio.packaging, moq_msf::Packaging::Legacy);
		assert_eq!(audio.codec, Some("opus".to_string()));
		assert_eq!(audio.samplerate, Some(48_000));
		assert_eq!(audio.channel_config, Some("2".to_string()));
		assert_eq!(audio.bitrate, Some(128_000));
	}

	#[test]
	fn convert_with_description() {
		let mut video_renditions = BTreeMap::new();
		video_renditions.insert(
			"video0.m4s".to_string(),
			VideoConfig {
				codec: H264 {
					profile: 0x64,
					constraints: 0x00,
					level: 0x1f,
					inline: false,
				}
				.into(),
				description: Some(Bytes::from_static(&[0x01, 0x02, 0x03])),
				coded_width: Some(1920),
				coded_height: Some(1080),
				display_ratio_width: None,
				display_ratio_height: None,
				bitrate: None,
				framerate: None,
				optimize_for_latency: None,
				container: Container::Legacy,
				jitter: None,
			},
		);

		let catalog = hang::Catalog {
			video: Video {
				renditions: video_renditions,
				display: None,
				rotation: None,
				flip: None,
			},
			..Default::default()
		};

		let msf = to_msf(&catalog);
		let video = &msf.tracks[0];
		assert_eq!(video.init_data, Some("AQID".to_string()));
	}

	#[test]
	fn convert_empty() {
		let catalog = hang::Catalog::default();
		let msf = to_msf(&catalog);
		assert_eq!(msf.version, 1);
		assert!(msf.tracks.is_empty());
	}

	#[test]
	fn convert_cmaf_packaging() {
		let mut video_renditions = BTreeMap::new();
		video_renditions.insert(
			"video0.m4s".to_string(),
			VideoConfig {
				codec: H264 {
					profile: 0x64,
					constraints: 0x00,
					level: 0x28,
					inline: false,
				}
				.into(),
				description: None,
				coded_width: Some(1920),
				coded_height: Some(1080),
				display_ratio_width: None,
				display_ratio_height: None,
				bitrate: None,
				framerate: None,
				optimize_for_latency: None,
				container: Container::Cmaf {
					init_data: "AAAYZ2Z0eXA=".to_string(),
				},
				jitter: None,
			},
		);

		let catalog = hang::Catalog {
			video: Video {
				renditions: video_renditions,
				display: None,
				rotation: None,
				flip: None,
			},
			..Default::default()
		};

		let msf = to_msf(&catalog);
		let video = &msf.tracks[0];
		assert_eq!(video.packaging, moq_msf::Packaging::Cmaf);
		assert_eq!(video.init_data, Some("AAAYZ2Z0eXA=".to_string()));
		// CMAF video tracks get CMSF SAP fields
		assert_eq!(video.max_grp_sap_starting_type, Some(1));
		assert_eq!(video.max_obj_sap_starting_type, Some(1));
	}

	#[test]
	fn sap_timeline_track_entry() {
		let track = sap_timeline_track("sap-events", Some("test-ns"));
		assert_eq!(track.packaging, moq_msf::Packaging::EventTimeline);
		assert_eq!(track.event_type.as_deref(), Some(CMSF_SAP_EVENT_TYPE));
		assert_eq!(track.namespace.as_deref(), Some("test-ns"));
	}

	#[test]
	fn sap_event_json_format() {
		let json = sap_event_json(1, 12345);
		assert_eq!(json, "{\"l\":[1,12345]}");
	}
}
