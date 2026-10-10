//! Binary encoding of the values stored in the redb snapshot.
//!
//! # Format
//!
//! The body of every value uses one fixed layout, defined here and
//! implemented in this file with no third-party encoder:
//!
//! - integers are fixed width, little endian (`usize`/`isize` as 64 bits);
//! - `f32`/`f64` are their IEEE-754 bits, little endian;
//! - `bool` is one byte, `0` or `1`;
//! - strings, byte strings, sequences and maps start with a `u64` length
//!   (bytes for strings, elements or entries otherwise);
//! - `Option` is a one-byte tag (`0` none, `1` some) followed by the value;
//! - structs and tuples are their fields in declaration order, no length;
//! - enums are a `u32` variant index followed by the variant's fields;
//! - `char` is its UTF-8 encoding.
//!
//! This is byte for byte the layout that `bincode` 1.x produced with its
//! default (`bincode::serialize`) configuration, which is what every redb
//! snapshot written before this module existed contains. The `bincode`
//! crate is unmaintained (RUSTSEC-2025-0141), so it was replaced by this
//! in-tree implementation of the same layout.
//!
//! # Versioning
//!
//! New values start with the 8-byte header [`HEADER`] (`"DASHv2\0\xff"`),
//! followed by the body. Values without the header are legacy values
//! written by `bincode` 1.x and are decoded with the same body rules, so
//! snapshots written by older releases load unchanged.
//!
//! The header cannot be mistaken for the start of a legacy value: every
//! stored type begins with a `u64` (a string length, a sequence length or
//! a counter), and the header read as a little-endian `u64` is larger than
//! `0xff00_0000_0000_0000`, which no length or counter can reach.
//!
//! A value whose first four bytes are `DASH` and whose eighth byte is
//! `0xff` but that carries another version is refused, so a snapshot
//! written by a newer release is never misread.
//!
//! Downgrade note: releases that still use `bincode` cannot read values
//! that carry the header. The snapshot is a materialized view of the WAL,
//! so after a downgrade delete the redb file and let the service rebuild
//! it from the WAL.

use serde::de::{
    self, DeserializeOwned, DeserializeSeed, EnumAccess, IntoDeserializer, MapAccess, SeqAccess,
    VariantAccess, Visitor,
};
use serde::ser::{self, Serialize};

/// Header that marks a value written in format version 2.
pub(crate) const HEADER: [u8; 8] = *b"DASHv2\x00\xff";

/// Encoding or decoding failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CodecError(String);

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CodecError {}

impl ser::Error for CodecError {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Self(msg.to_string())
    }
}

impl de::Error for CodecError {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Self(msg.to_string())
    }
}

type Result<T> = std::result::Result<T, CodecError>;

/// Encode `value` as a format-version-2 value (header + body).
pub(crate) fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(&HEADER);
    value.serialize(&mut Encoder { out: &mut out })?;
    Ok(out)
}

/// Encode only the body (no header). Used by tests to compare against
/// bytes produced by `bincode` 1.x.
#[cfg(test)]
pub(crate) fn encode_body<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    value.serialize(&mut Encoder { out: &mut out })?;
    Ok(out)
}

/// Decode a value written either by [`encode`] or, without a header, by
/// `bincode` 1.x.
pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let (body, legacy) = split_header(bytes)?;
    let mut decoder = Decoder {
        input: body,
        legacy,
    };
    let value = T::deserialize(&mut decoder)?;
    // bincode 1.x ignored trailing bytes; keep that for legacy values.
    // Versioned values must be consumed exactly.
    if !legacy && !decoder.input.is_empty() {
        return Err(CodecError(format!(
            "{} trailing bytes after value",
            decoder.input.len()
        )));
    }
    Ok(value)
}

/// Returns the body and whether the value is a legacy (headerless) value.
fn split_header(bytes: &[u8]) -> Result<(&[u8], bool)> {
    if bytes.len() >= HEADER.len() && bytes[..4] == HEADER[..4] && bytes[7] == HEADER[7] {
        if bytes[..HEADER.len()] == HEADER {
            return Ok((&bytes[HEADER.len()..], false));
        }
        return Err(CodecError(format!(
            "unsupported value format header {:?}",
            String::from_utf8_lossy(&bytes[..7])
        )));
    }
    Ok((bytes, true))
}

// ---------------------------------------------------------------------------
// Encoder
// ---------------------------------------------------------------------------

struct Encoder<'a> {
    out: &'a mut Vec<u8>,
}

impl Encoder<'_> {
    fn len(&mut self, len: usize) {
        self.out.extend_from_slice(&(len as u64).to_le_bytes());
    }

    fn variant(&mut self, index: u32) {
        self.out.extend_from_slice(&index.to_le_bytes());
    }
}

impl<'a, 'b> ser::Serializer for &'a mut Encoder<'b> {
    type Ok = ();
    type Error = CodecError;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn is_human_readable(&self) -> bool {
        false
    }

    fn serialize_bool(self, v: bool) -> Result<()> {
        self.out.push(u8::from(v));
        Ok(())
    }
    fn serialize_i8(self, v: i8) -> Result<()> {
        self.out.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }
    fn serialize_i16(self, v: i16) -> Result<()> {
        self.out.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }
    fn serialize_i32(self, v: i32) -> Result<()> {
        self.out.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }
    fn serialize_i64(self, v: i64) -> Result<()> {
        self.out.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }
    fn serialize_i128(self, v: i128) -> Result<()> {
        self.out.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }
    fn serialize_u8(self, v: u8) -> Result<()> {
        self.out.push(v);
        Ok(())
    }
    fn serialize_u16(self, v: u16) -> Result<()> {
        self.out.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }
    fn serialize_u32(self, v: u32) -> Result<()> {
        self.out.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }
    fn serialize_u64(self, v: u64) -> Result<()> {
        self.out.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }
    fn serialize_u128(self, v: u128) -> Result<()> {
        self.out.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }
    fn serialize_f32(self, v: f32) -> Result<()> {
        self.out.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }
    fn serialize_f64(self, v: f64) -> Result<()> {
        self.out.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }
    fn serialize_char(self, v: char) -> Result<()> {
        let mut buf = [0u8; 4];
        self.out
            .extend_from_slice(v.encode_utf8(&mut buf).as_bytes());
        Ok(())
    }
    fn serialize_str(self, v: &str) -> Result<()> {
        self.len(v.len());
        self.out.extend_from_slice(v.as_bytes());
        Ok(())
    }
    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        self.len(v.len());
        self.out.extend_from_slice(v);
        Ok(())
    }
    fn serialize_none(self) -> Result<()> {
        self.out.push(0);
        Ok(())
    }
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<()> {
        self.out.push(1);
        value.serialize(self)
    }
    fn serialize_unit(self) -> Result<()> {
        Ok(())
    }
    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        Ok(())
    }
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
    ) -> Result<()> {
        self.variant(variant_index);
        Ok(())
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<()> {
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        value: &T,
    ) -> Result<()> {
        self.variant(variant_index);
        value.serialize(self)
    }
    fn serialize_seq(self, len: Option<usize>) -> Result<Self> {
        let len = len.ok_or_else(|| CodecError("sequence length must be known".into()))?;
        self.len(len);
        Ok(self)
    }
    fn serialize_tuple(self, _len: usize) -> Result<Self> {
        Ok(self)
    }
    fn serialize_tuple_struct(self, _name: &'static str, _len: usize) -> Result<Self> {
        Ok(self)
    }
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self> {
        self.variant(variant_index);
        Ok(self)
    }
    fn serialize_map(self, len: Option<usize>) -> Result<Self> {
        let len = len.ok_or_else(|| CodecError("map length must be known".into()))?;
        self.len(len);
        Ok(self)
    }
    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self> {
        Ok(self)
    }
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self> {
        self.variant(variant_index);
        Ok(self)
    }
}

impl ser::SerializeSeq for &mut Encoder<'_> {
    type Ok = ();
    type Error = CodecError;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl ser::SerializeTuple for &mut Encoder<'_> {
    type Ok = ();
    type Error = CodecError;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl ser::SerializeTupleStruct for &mut Encoder<'_> {
    type Ok = ();
    type Error = CodecError;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl ser::SerializeTupleVariant for &mut Encoder<'_> {
    type Ok = ();
    type Error = CodecError;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl ser::SerializeMap for &mut Encoder<'_> {
    type Ok = ();
    type Error = CodecError;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<()> {
        key.serialize(&mut **self)
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl ser::SerializeStruct for &mut Encoder<'_> {
    type Ok = ();
    type Error = CodecError;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        _key: &'static str,
        value: &T,
    ) -> Result<()> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl ser::SerializeStructVariant for &mut Encoder<'_> {
    type Ok = ();
    type Error = CodecError;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        _key: &'static str,
        value: &T,
    ) -> Result<()> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Decoder
// ---------------------------------------------------------------------------

struct Decoder<'de> {
    input: &'de [u8],
    /// Headerless value written by bincode 1.x. Struct fields missing at the
    /// very end of such a value take their `#[serde(default)]`, so values
    /// written before a defaulted field was added still load.
    legacy: bool,
}

impl<'de> Decoder<'de> {
    fn take(&mut self, n: usize) -> Result<&'de [u8]> {
        if n > self.input.len() {
            return Err(CodecError(format!(
                "unexpected end of value: need {n} bytes, {} left",
                self.input.len()
            )));
        }
        let (head, rest) = self.input.split_at(n);
        self.input = rest;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }

    /// A `u64` length that must not exceed the bytes left (every element
    /// occupies at least one byte except zero-sized ones, which the stored
    /// types do not contain).
    fn len(&mut self) -> Result<usize> {
        let raw = u64::from_le_bytes(self.array()?);
        let len =
            usize::try_from(raw).map_err(|_| CodecError(format!("length {raw} too large")))?;
        if len > self.input.len() {
            return Err(CodecError(format!(
                "length {len} exceeds the {} bytes left",
                self.input.len()
            )));
        }
        Ok(len)
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }
}

impl<'de> de::Deserializer<'de> for &mut Decoder<'de> {
    type Error = CodecError;

    fn is_human_readable(&self) -> bool {
        false
    }

    fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        Err(CodecError("the value format is not self-describing".into()))
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.take(1)?[0] {
            0 => visitor.visit_bool(false),
            1 => visitor.visit_bool(true),
            other => Err(CodecError(format!("invalid bool byte {other}"))),
        }
    }
    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i8(i8::from_le_bytes(self.array()?))
    }
    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i16(i16::from_le_bytes(self.array()?))
    }
    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i32(i32::from_le_bytes(self.array()?))
    }
    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i64(i64::from_le_bytes(self.array()?))
    }
    fn deserialize_i128<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i128(i128::from_le_bytes(self.array()?))
    }
    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u8(self.take(1)?[0])
    }
    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u16(u16::from_le_bytes(self.array()?))
    }
    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u32(self.u32()?)
    }
    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u64(u64::from_le_bytes(self.array()?))
    }
    fn deserialize_u128<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u128(u128::from_le_bytes(self.array()?))
    }
    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_f32(f32::from_le_bytes(self.array()?))
    }
    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_f64(f64::from_le_bytes(self.array()?))
    }
    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let first = *self
            .input
            .first()
            .ok_or_else(|| CodecError("unexpected end of value: char".into()))?;
        let width = match first {
            0x00..=0x7f => 1,
            0xc0..=0xdf => 2,
            0xe0..=0xef => 3,
            0xf0..=0xf7 => 4,
            _ => return Err(CodecError("invalid char encoding".into())),
        };
        let bytes = self.take(width)?;
        let s = std::str::from_utf8(bytes).map_err(|e| CodecError(format!("invalid char: {e}")))?;
        let c = s
            .chars()
            .next()
            .ok_or_else(|| CodecError("invalid char encoding".into()))?;
        visitor.visit_char(c)
    }
    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let len = self.len()?;
        let bytes = self.take(len)?;
        let s = std::str::from_utf8(bytes)
            .map_err(|e| CodecError(format!("invalid UTF-8 in string: {e}")))?;
        visitor.visit_borrowed_str(s)
    }
    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_str(visitor)
    }
    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let len = self.len()?;
        visitor.visit_borrowed_bytes(self.take(len)?)
    }
    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_bytes(visitor)
    }
    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.take(1)?[0] {
            0 => visitor.visit_none(),
            1 => visitor.visit_some(self),
            other => Err(CodecError(format!("invalid option tag {other}"))),
        }
    }
    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_unit()
    }
    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_unit()
    }
    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_newtype_struct(self)
    }
    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let len = self.len()?;
        visitor.visit_seq(Access {
            de: self,
            left: len,
            lenient_tail: false,
        })
    }
    fn deserialize_tuple<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value> {
        visitor.visit_seq(Access {
            de: self,
            left: len,
            lenient_tail: false,
        })
    }
    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value> {
        self.deserialize_tuple(len, visitor)
    }
    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let len = self.len()?;
        visitor.visit_map(Access {
            de: self,
            left: len,
            lenient_tail: false,
        })
    }
    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        let lenient_tail = self.legacy;
        visitor.visit_seq(Access {
            de: self,
            left: fields.len(),
            lenient_tail,
        })
    }
    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_enum(self)
    }
    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u32(self.u32()?)
    }
    fn deserialize_ignored_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        Err(CodecError(
            "the value format cannot skip unknown data".into(),
        ))
    }
}

struct Access<'a, 'de> {
    de: &'a mut Decoder<'de>,
    left: usize,
    /// Report the end of a struct when the input runs out at a field
    /// boundary (legacy values only), so trailing `#[serde(default)]`
    /// fields take their default.
    lenient_tail: bool,
}

impl<'de> SeqAccess<'de> for Access<'_, 'de> {
    type Error = CodecError;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>> {
        if self.left == 0 {
            return Ok(None);
        }
        if self.lenient_tail && self.de.input.is_empty() {
            self.left = 0;
            return Ok(None);
        }
        self.left -= 1;
        seed.deserialize(&mut *self.de).map(Some)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.left)
    }
}

impl<'de> MapAccess<'de> for Access<'_, 'de> {
    type Error = CodecError;

    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>> {
        if self.left == 0 {
            return Ok(None);
        }
        self.left -= 1;
        seed.deserialize(&mut *self.de).map(Some)
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value> {
        seed.deserialize(&mut *self.de)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.left)
    }
}

impl<'de> EnumAccess<'de> for &mut Decoder<'de> {
    type Error = CodecError;
    type Variant = Self;

    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Self)> {
        let index = self.u32()?;
        let value = seed.deserialize(index.into_deserializer())?;
        Ok((value, self))
    }
}

impl<'de> VariantAccess<'de> for &mut Decoder<'de> {
    type Error = CodecError;

    fn unit_variant(self) -> Result<()> {
        Ok(())
    }
    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value> {
        seed.deserialize(self)
    }
    fn tuple_variant<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value> {
        visitor.visit_seq(Access {
            de: self,
            left: len,
            lenient_tail: false,
        })
    }
    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_seq(Access {
            de: self,
            left: fields.len(),
            lenient_tail: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BatchCommitMetadata, StoreIndexStats};
    use schema::{Claim, ClaimEdge, ClaimType, Evidence, Relation, Stance};

    /// Bytes written by `bincode` 1.3.3 (`bincode::serialize`) before it was
    /// removed. Generated once from the values in `fixture_values` and
    /// committed; see `tests/fixtures/README.md`.
    const LEGACY_HEX: &str = include_str!("../tests/fixtures/legacy-bincode-v1.hex");

    fn legacy(name: &str) -> Vec<u8> {
        let line = LEGACY_HEX
            .lines()
            .find_map(|l| l.strip_prefix(&format!("{name}=")))
            .unwrap_or_else(|| panic!("fixture {name} missing"));
        (0..line.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&line[i..i + 2], 16).unwrap())
            .collect()
    }

    pub(crate) fn full_claim() -> Claim {
        Claim {
            claim_id: "claim-full".into(),
            tenant_id: "tenant-a".into(),
            canonical_text: "Café revenue grew 12% in Q3 — ünïcödé ✓".into(),
            confidence: 0.875,
            event_time_unix: Some(-86_400),
            entities: vec!["Café".into(), "Q3".into()],
            embedding_ids: vec!["emb-1".into()],
            claim_type: Some(ClaimType::Temporal),
            valid_from: Some(1_700_000_000),
            valid_to: Some(i64::MAX),
            created_at: Some(1_700_000_001),
            updated_at: Some(1_700_000_002),
        }
    }

    fn evidence() -> Vec<Evidence> {
        vec![
            Evidence {
                evidence_id: "ev-1".into(),
                claim_id: "claim-full".into(),
                source_id: "source://report".into(),
                stance: Stance::Contradicts,
                source_quality: 0.25,
                chunk_id: Some("chunk-9".into()),
                span_start: Some(3),
                span_end: Some(u32::MAX),
                doc_id: Some("doc-7".into()),
                extraction_model: Some("extractor-v2".into()),
                ingested_at: Some(1_700_000_003),
            },
            Evidence {
                evidence_id: "ev-2".into(),
                claim_id: "claim-full".into(),
                source_id: "source://x".into(),
                stance: Stance::Neutral,
                source_quality: 1.0,
                chunk_id: None,
                span_start: None,
                span_end: None,
                doc_id: None,
                extraction_model: None,
                ingested_at: None,
            },
        ]
    }

    fn edges() -> Vec<ClaimEdge> {
        vec![ClaimEdge {
            edge_id: "edge-1".into(),
            from_claim_id: "claim-full".into(),
            to_claim_id: "claim-min".into(),
            relation: Relation::DependsOn,
            strength: -0.5,
            reason_codes: vec!["temporal".into(), "entity-overlap".into()],
            created_at: Some(42),
        }]
    }

    fn vector() -> Vec<f32> {
        vec![0.1f32, -2.5, f32::MIN_POSITIVE, 1e30]
    }

    fn commit() -> BatchCommitMetadata {
        BatchCommitMetadata {
            commit_id: "commit-1".into(),
            batch_size: 2,
            ts_unix_ms: 1_700_000_000_123,
            claim_ids: vec!["claim-full".into(), "claim-min".into()],
            payload_fingerprint: "sha256:abcdef".into(),
        }
    }

    fn stats() -> StoreIndexStats {
        StoreIndexStats {
            tenant_count: 2,
            claim_count: 2,
            vector_count: 1,
            inverted_terms: 11,
            entity_terms: 2,
            temporal_buckets: 1,
            ann_vector_buckets: 1,
            vector_index_bytes: 4096,
        }
    }

    #[test]
    fn legacy_bincode_values_decode_to_the_original_values() {
        assert_eq!(
            decode::<Claim>(&legacy("claim_full")).unwrap(),
            full_claim()
        );
        assert_eq!(
            decode::<Claim>(&legacy("claim_min")).unwrap(),
            schema::claim_builder("claim-min", "tenant-b", "plain", 0.5)
        );
        assert_eq!(
            decode::<Vec<Evidence>>(&legacy("evidence")).unwrap(),
            evidence()
        );
        assert_eq!(decode::<Vec<ClaimEdge>>(&legacy("edges")).unwrap(), edges());
        assert_eq!(decode::<Vec<f32>>(&legacy("vector")).unwrap(), vector());
        assert_eq!(
            decode::<BatchCommitMetadata>(&legacy("commit")).unwrap(),
            commit()
        );
        assert_eq!(
            decode::<StoreIndexStats>(&legacy("stats")).unwrap(),
            stats()
        );
    }

    #[test]
    fn body_layout_is_byte_identical_to_bincode_1() {
        assert_eq!(encode_body(&full_claim()).unwrap(), legacy("claim_full"));
        assert_eq!(encode_body(&evidence()).unwrap(), legacy("evidence"));
        assert_eq!(encode_body(&edges()).unwrap(), legacy("edges"));
        assert_eq!(encode_body(&vector()).unwrap(), legacy("vector"));
        assert_eq!(encode_body(&commit()).unwrap(), legacy("commit"));
        assert_eq!(encode_body(&stats()).unwrap(), legacy("stats"));
    }

    #[test]
    fn new_values_carry_the_header_and_round_trip() {
        let bytes = encode(&full_claim()).unwrap();
        assert_eq!(&bytes[..8], &HEADER);
        assert_eq!(decode::<Claim>(&bytes).unwrap(), full_claim());
        assert_eq!(
            decode::<Vec<Evidence>>(&encode(&evidence()).unwrap()).unwrap(),
            evidence()
        );
        assert_eq!(
            decode::<Vec<ClaimEdge>>(&encode(&edges()).unwrap()).unwrap(),
            edges()
        );
        assert_eq!(
            decode::<Vec<f32>>(&encode(&vector()).unwrap()).unwrap(),
            vector()
        );
        assert_eq!(
            decode::<BatchCommitMetadata>(&encode(&commit()).unwrap()).unwrap(),
            commit()
        );
        assert_eq!(
            decode::<StoreIndexStats>(&encode(&stats()).unwrap()).unwrap(),
            stats()
        );
    }

    #[test]
    fn header_read_as_a_legacy_length_is_impossibly_large() {
        assert!(u64::from_le_bytes(HEADER) > 0xff00_0000_0000_0000);
    }

    #[test]
    fn unknown_format_version_is_refused() {
        let mut bytes = encode(&stats()).unwrap();
        bytes[5] = b'9';
        let err = decode::<StoreIndexStats>(&bytes).unwrap_err();
        assert!(
            err.to_string().contains("unsupported value format"),
            "{err}"
        );
    }

    #[test]
    fn truncated_and_trailing_versioned_values_are_rejected() {
        let bytes = encode(&full_claim()).unwrap();
        for cut in [HEADER.len(), HEADER.len() + 3, bytes.len() - 1] {
            assert!(decode::<Claim>(&bytes[..cut]).is_err(), "cut at {cut}");
        }
        let mut extra = bytes.clone();
        extra.push(0);
        assert!(decode::<Claim>(&extra).is_err());
    }

    #[test]
    fn corrupt_lengths_and_tags_are_errors_not_panics() {
        // Length larger than the value.
        let mut bytes = legacy("vector");
        bytes[0] = 0xff;
        assert!(decode::<Vec<f32>>(&bytes).is_err());
        // Option tag that is neither 0 nor 1 (claim_min: the byte after
        // confidence is the event_time_unix tag).
        let mut bytes = legacy("claim_min");
        let tag_at = 8 + 9 + 8 + 8 + 8 + 5 + 4;
        assert_eq!(bytes[tag_at], 0);
        bytes[tag_at] = 7;
        assert!(decode::<Claim>(&bytes).is_err());
        // Invalid UTF-8 inside a string.
        let mut bytes = legacy("claim_min");
        bytes[8] = 0xff;
        assert!(decode::<Claim>(&bytes).is_err());
        // Enum variant index out of range.
        let mut bytes = legacy("edges");
        let rel_at = 8 + (8 + 6) + (8 + 10) + (8 + 9);
        assert_eq!(bytes[rel_at], 4);
        bytes[rel_at] = 99;
        assert!(decode::<Vec<ClaimEdge>>(&bytes).is_err());
    }

    #[test]
    fn legacy_stats_written_before_vector_index_bytes_existed_still_load() {
        let bytes = legacy("stats");
        let older = &bytes[..bytes.len() - 8];
        let decoded: StoreIndexStats = decode(older).unwrap();
        assert_eq!(
            decoded,
            StoreIndexStats {
                vector_index_bytes: 0,
                ..stats()
            }
        );
    }
}
