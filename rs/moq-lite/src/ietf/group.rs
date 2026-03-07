use crate::coding::{Decode, DecodeError, Encode, EncodeError};

use num_enum::{IntoPrimitive, TryFromPrimitive};

use super::Version;
use crate::ietf::Param;

#[derive(Debug, Clone, Copy, PartialEq, Eq, TryFromPrimitive, IntoPrimitive)]
#[repr(u8)]
pub enum GroupOrder {
	Any = 0x0,
	Ascending = 0x1,
	Descending = 0x2,
}

impl GroupOrder {
	/// Map `Any` (0x0) to `Descending`, leaving other values unchanged.
	pub fn any_to_descending(self) -> Self {
		match self {
			Self::Any => Self::Descending,
			other => other,
		}
	}
}

impl Encode<Version> for GroupOrder {
	fn encode<W: bytes::BufMut>(&self, w: &mut W, version: Version) -> Result<(), EncodeError> {
		u8::from(*self).encode(w, version)?;
		Ok(())
	}
}

impl Decode<Version> for GroupOrder {
	fn decode<R: bytes::Buf>(r: &mut R, version: Version) -> Result<Self, DecodeError> {
		Self::try_from(u8::decode(r, version)?).map_err(|_| DecodeError::InvalidValue)
	}
}

impl Param for GroupOrder {
	fn param_encode<W: bytes::BufMut>(&self, w: &mut W, version: Version) -> Result<(), EncodeError> {
		u8::from(*self).param_encode(w, version)
	}

	fn param_decode<R: bytes::Buf>(r: &mut R, version: Version) -> Result<Self, DecodeError> {
		let v = u8::param_decode(r, version)?;
		Ok(GroupOrder::try_from(v)
			.unwrap_or(GroupOrder::Descending)
			.any_to_descending())
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupFlags {
	// The group has extensions.
	pub has_extensions: bool,

	// There's an explicit subgroup on the wire.
	pub has_subgroup: bool,

	// Use the first object ID as the subgroup ID
	// Since we don't support subgroups or object ID > 0, this is trivial to support.
	// Not compatibile with has_subgroup
	pub has_subgroup_object: bool,

	// There's an implicit end marker when the stream is closed.
	pub has_end: bool,

	// v15: whether priority is present in the header.
	// When false (0x30 base), priority inherits from the control message.
	pub has_priority: bool,
}

impl GroupFlags {
	// v14 range: 0x08-0x0d (priority always present, no has_end bit)
	// Compatible with Shaka Player's draft-14 implementation.
	pub const START: u64 = 0x08;
	pub const END: u64 = 0x0d;

	// Legacy v14 range (0x10-0x1d) accepted for decode backward compat.
	pub const LEGACY_START: u64 = 0x10;
	pub const LEGACY_END: u64 = 0x1d;

	// v15 adds: 0x30-0x3d (priority absent, inherits from control message)
	pub const START_NO_PRIORITY: u64 = 0x30;
	pub const END_NO_PRIORITY: u64 = 0x3d;

	pub fn encode(&self) -> Result<u64, EncodeError> {
		if self.has_subgroup && self.has_subgroup_object {
			return Err(EncodeError::InvalidState);
		}

		let base = if self.has_priority {
			Self::START
		} else {
			Self::START_NO_PRIORITY
		};
		let mut id: u64 = base;
		if self.has_extensions {
			id |= 0x01;
		}
		if self.has_subgroup_object {
			id |= 0x02;
		}
		if self.has_subgroup {
			id |= 0x04;
		}
		// has_end is only encoded for the v15 no-priority range (0x30+).
		// The v14 range (0x08+) has no has_end bit — end is implicit from stream FIN.
		if !self.has_priority && self.has_end {
			id |= 0x08;
		}
		Ok(id)
	}

	pub fn decode(id: u64) -> Result<Self, DecodeError> {
		let (has_priority, base_id) = if (Self::START..=Self::END).contains(&id) {
			// v14 range: 0x08-0x0d (no has_end bit)
			(true, id - Self::START)
		} else if (Self::LEGACY_START..=Self::LEGACY_END).contains(&id) {
			// Legacy v14 range: 0x10-0x1d (has has_end bit at bit 3)
			(true, id - Self::LEGACY_START)
		} else if (Self::START_NO_PRIORITY..=Self::END_NO_PRIORITY).contains(&id) {
			(false, id - Self::START_NO_PRIORITY)
		} else {
			return Err(DecodeError::InvalidValue);
		};

		let has_extensions = (base_id & 0x01) != 0;
		let has_subgroup_object = (base_id & 0x02) != 0;
		let has_subgroup = (base_id & 0x04) != 0;
		// has_end is at bit 3 for legacy v14 range and v15 no-priority range.
		// For the v14 range (0x08-0x0d) it's always true (implicit from stream FIN).
		let has_end = if (Self::START..=Self::END).contains(&id) {
			true // v14: implicit from stream FIN
		} else {
			(base_id & 0x08) != 0 // legacy and no-priority: explicit bit
		};

		if has_subgroup && has_subgroup_object {
			return Err(DecodeError::InvalidValue);
		}

		Ok(Self {
			has_extensions,
			has_subgroup,
			has_subgroup_object,
			has_end,
			has_priority,
		})
	}
}

impl Default for GroupFlags {
	fn default() -> Self {
		Self {
			has_extensions: false,
			has_subgroup: false,
			has_subgroup_object: false,
			has_end: true,
			has_priority: true,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupHeader {
	pub track_alias: u64,
	pub group_id: u64,
	pub sub_group_id: u64,
	pub publisher_priority: u8,
	pub flags: GroupFlags,
}

impl Encode<Version> for GroupHeader {
	fn encode<W: bytes::BufMut>(&self, w: &mut W, version: Version) -> Result<(), EncodeError> {
		self.flags.encode()?.encode(w, version)?;
		self.track_alias.encode(w, version)?;
		self.group_id.encode(w, version)?;

		if !self.flags.has_subgroup && self.sub_group_id != 0 {
			return Err(EncodeError::InvalidState);
		}

		if self.flags.has_subgroup {
			self.sub_group_id.encode(w, version)?;
		}

		// Publisher priority (only if has_priority flag is set)
		if self.flags.has_priority {
			self.publisher_priority.encode(w, version)?;
		}
		Ok(())
	}
}

impl Decode<Version> for GroupHeader {
	fn decode<R: bytes::Buf>(r: &mut R, version: Version) -> Result<Self, DecodeError> {
		let flags = GroupFlags::decode(u64::decode(r, version)?)?;
		let track_alias = u64::decode(r, version)?;
		let group_id = u64::decode(r, version)?;

		let sub_group_id = match flags.has_subgroup {
			true => u64::decode(r, version)?,
			false => 0,
		};

		// Priority present only if has_priority flag is set
		let publisher_priority = if flags.has_priority {
			u8::decode(r, version)?
		} else {
			128 // Default priority when absent
		};

		Ok(Self {
			track_alias,
			group_id,
			sub_group_id,
			publisher_priority,
			flags,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_group_flags_v14_encode() {
		// Default flags encode to 0x08 (base, no extras)
		let flags = GroupFlags::default();
		assert_eq!(flags.encode().unwrap(), 0x08);

		// With extensions: 0x09
		let flags = GroupFlags { has_extensions: true, ..Default::default() };
		assert_eq!(flags.encode().unwrap(), 0x09);

		// With subgroup_object: 0x0A
		let flags = GroupFlags { has_subgroup_object: true, ..Default::default() };
		assert_eq!(flags.encode().unwrap(), 0x0A);

		// With explicit subgroup: 0x0C
		let flags = GroupFlags { has_subgroup: true, ..Default::default() };
		assert_eq!(flags.encode().unwrap(), 0x0C);

		// With subgroup + extensions: 0x0D
		let flags = GroupFlags { has_subgroup: true, has_extensions: true, ..Default::default() };
		assert_eq!(flags.encode().unwrap(), 0x0D);

		// Invalid: both subgroup and subgroup_object
		let flags = GroupFlags { has_subgroup: true, has_subgroup_object: true, ..Default::default() };
		assert!(flags.encode().is_err());
	}

	#[test]
	fn test_group_flags_v14_decode() {
		// 0x08: base
		let flags = GroupFlags::decode(0x08).unwrap();
		assert!(!flags.has_subgroup);
		assert!(!flags.has_subgroup_object);
		assert!(!flags.has_extensions);
		assert!(flags.has_end); // implicit
		assert!(flags.has_priority);

		// 0x09: extensions
		let flags = GroupFlags::decode(0x09).unwrap();
		assert!(flags.has_extensions);
		assert!(!flags.has_subgroup);

		// 0x0A: subgroup_object
		let flags = GroupFlags::decode(0x0A).unwrap();
		assert!(flags.has_subgroup_object);

		// 0x0C: explicit subgroup
		let flags = GroupFlags::decode(0x0C).unwrap();
		assert!(flags.has_subgroup);

		// 0x0D: subgroup + extensions
		let flags = GroupFlags::decode(0x0D).unwrap();
		assert!(flags.has_subgroup);
		assert!(flags.has_extensions);

		// Invalid: 0x0E = subgroup + subgroup_object
		assert!(GroupFlags::decode(0x0E).is_err());
	}

	#[test]
	fn test_group_flags_legacy_decode() {
		// Legacy 0x10-0x1D range is still accepted for backward compat
		let flags = GroupFlags::decode(0x10).unwrap();
		assert!(!flags.has_subgroup);
		assert!(!flags.has_extensions);
		assert!(!flags.has_end); // legacy: bit 3 = 0
		assert!(flags.has_priority);

		let flags = GroupFlags::decode(0x18).unwrap();
		assert!(flags.has_end); // legacy: bit 3 = 1
		assert!(flags.has_priority);

		let flags = GroupFlags::decode(0x1D).unwrap();
		assert!(flags.has_subgroup);
		assert!(flags.has_extensions);
		assert!(flags.has_end);

		// Invalid in legacy range too
		assert!(GroupFlags::decode(0x16).is_err());
	}

	#[test]
	fn test_group_flags_no_priority_range() {
		// v15: 0x30 range = no priority, has_end at bit 3
		let flags = GroupFlags::decode(0x30).unwrap();
		assert!(!flags.has_priority);
		assert!(!flags.has_subgroup);
		assert!(!flags.has_extensions);
		assert!(!flags.has_end); // bit 3 = 0
		assert_eq!(flags.encode().unwrap(), 0x30);

		// 0x38 = no-priority with has_end
		let flags = GroupFlags::decode(0x38).unwrap();
		assert!(!flags.has_priority);
		assert!(flags.has_end); // bit 3 = 1
		assert_eq!(flags.encode().unwrap(), 0x38);

		// 0x3D = no-priority, subgroup + extensions + has_end
		let flags = GroupFlags::decode(0x3D).unwrap();
		assert!(!flags.has_priority);
		assert!(flags.has_subgroup);
		assert!(flags.has_extensions);
		assert!(flags.has_end);
		assert_eq!(flags.encode().unwrap(), 0x3D);

		// Invalid: Both has_subgroup and has_subgroup_object in no-priority range
		assert!(GroupFlags::decode(0x36).is_err());
	}
}
