// START_AI_HEADER
// MODULE: zenoh-util-freebsd/src/ffi/mod.rs
// PURPOSE: FFI utilities for string conversion and safe JSON exchange between Zenoh plugins.
// INTENT: Provides safe wrappers around raw pointer string conversion (PCWSTR/PSTR) and serialization-safe JSON containers for plugin ABI.
// DEPENDENCIES: std, serde, serde_json, schemars (via JsonValue derive)
// PUBLIC_API: pwstr_to_string, pstr_to_string, JsonValue, JsonKeyValueMap
// END_AI_HEADER

//
// Copyright (c) 2023 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//
#[cfg(windows)]
pub mod win;

/// # Safety
/// Dereferences raw pointer argument.
/// ptr should be a valid string pointer.
pub unsafe fn pwstr_to_string(ptr: *mut u16) -> String {
    use std::slice::from_raw_parts;

    // SAFETY: Dereference the raw pointer and call from_raw_parts.
    let array: &[u16] = unsafe {
        let len = (0_usize..)
            .find(|&n| *ptr.add(n) == 0)
            .expect("Couldn't find null terminator");
        from_raw_parts(ptr, len)
    };

    String::from_utf16_lossy(array)
}

/// # Safety
/// Dereferences raw pointer argument.
/// ptr should be a valid string pointer.
pub unsafe fn pstr_to_string(ptr: *mut i8) -> String {
    use std::slice::from_raw_parts;

    // SAFETY: Dereference the raw pointer and call from_raw_parts.
    let array: &[u8] = unsafe {
        let len = (0_usize..)
            .find(|&n| *ptr.add(n) == 0)
            .expect("Couldn't find null terminator");
        from_raw_parts(ptr as *const u8, len)
    };

    String::from_utf8_lossy(array).to_string()
}

/// Struct used to safely exchange data in json format in plugins.
/// It is not entirely abi stable due to using String, but should do
/// for now since we require plugins to use the same version of rustc and
/// same version of zenoh.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
#[serde(from = "serde_json::Value")]
#[serde(into = "serde_json::Value")]
pub struct JsonValue(String);

impl PartialEq for JsonValue {
    // eq:start
//   purpose: Compare two JsonValues by deserializing to serde_json::Value and comparing the parsed values.
//   input:  self, other - JsonValue instances.
//   output: bool - true if the JSON values are semantically equal.
//   sideEffects: none
    fn eq(&self, other: &Self) -> bool {
        let left: serde_json::Value = self.into();
        let right: serde_json::Value = other.into();
        left == right
    }
    // eq:end
}

impl Eq for JsonValue {}

impl From<&serde_json::Value> for JsonValue {
    // from:start
//   purpose: Serialize a borrowed serde_json::Value into an internal JSON string.
//   input:  value - borrowed serde_json::Value.
//   output: JsonValue wrapping the serialized JSON string.
//   sideEffects: none (serialization is infallible)
    fn from(value: &serde_json::Value) -> Self {
        JsonValue(serde_json::to_string(value).unwrap())
    }
    // from:end
}

impl From<serde_json::Value> for JsonValue {
    // from:start
//   purpose: Serialize an owned serde_json::Value into an internal JSON string.
//   input:  value - owned serde_json::Value.
//   output: JsonValue wrapping the serialized JSON string.
//   sideEffects: none
    fn from(value: serde_json::Value) -> Self {
        (&value).into()
    }
    // from:end
}

impl From<&JsonValue> for serde_json::Value {
    // from:start
//   purpose: Deserialize a borrowed JsonValue back into a serde_json::Value.
//   input:  value - borrowed JsonValue.
//   output: serde_json::Value parsed from the internal JSON string.
//   sideEffects: none (deserialization is infallible)
    fn from(value: &JsonValue) -> Self {
        serde_json::from_str(&value.0).unwrap()
    }
    // from:end
}

impl From<JsonValue> for serde_json::Value {
    // from:start
//   purpose: Deserialize an owned JsonValue back into a serde_json::Value.
//   input:  value - owned JsonValue.
//   output: serde_json::Value parsed from the internal JSON string.
//   sideEffects: none
    fn from(value: JsonValue) -> Self {
        (&value).into()
    }
    // from:end
}

impl Default for JsonValue {
    // default:start
//   purpose: Create a JsonValue representing the JSON null value.
//   input:  none.
//   output: JsonValue wrapping "null".
//   sideEffects: none
    fn default() -> Self {
        serde_json::Value::default().into()
    }
    // default:end
}

impl JsonValue {
    // into_serde_value:start
//   purpose: Convert the JsonValue into a serde_json::Value by deserializing its internal string.
//   input:  &self.
//   output: serde_json::Value.
//   sideEffects: none
    pub fn into_serde_value(&self) -> serde_json::Value {
        self.into()
    }
    // into_serde_value:end
}

/// Struct used to safely exchange data in json format in plugins.
/// It is not entirely abi stable due to using String, but should do
/// for now since we require plugins to use the same version of rustc and
/// same version of zenoh.
#[derive(Debug, Clone)]
pub struct JsonKeyValueMap(String);

impl From<&serde_json::Map<String, serde_json::Value>> for JsonKeyValueMap {
    // from:start
//   purpose: Serialize a borrowed JSON map into an internal JSON string.
//   input:  value - borrowed serde_json::Map.
//   output: JsonKeyValueMap wrapping the serialized JSON string.
//   sideEffects: none (serialization is infallible)
    fn from(value: &serde_json::Map<String, serde_json::Value>) -> Self {
        JsonKeyValueMap(serde_json::to_string(value).unwrap())
    }
    // from:end
}

impl From<serde_json::Map<String, serde_json::Value>> for JsonKeyValueMap {
    // from:start
//   purpose: Serialize an owned JSON map into an internal JSON string.
//   input:  value - owned serde_json::Map.
//   output: JsonKeyValueMap wrapping the serialized JSON string.
//   sideEffects: none
    fn from(value: serde_json::Map<String, serde_json::Value>) -> Self {
        (&value).into()
    }
    // from:end
}

impl From<&JsonKeyValueMap> for serde_json::Map<String, serde_json::Value> {
    // from:start
//   purpose: Deserialize a borrowed JsonKeyValueMap back into a serde_json::Map.
//   input:  value - borrowed JsonKeyValueMap.
//   output: serde_json::Map parsed from the internal JSON string.
//   sideEffects: none (deserialization is infallible)
    fn from(value: &JsonKeyValueMap) -> Self {
        serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&value.0).unwrap()
    }
    // from:end
}

impl From<JsonKeyValueMap> for serde_json::Map<String, serde_json::Value> {
    // from:start
//   purpose: Deserialize an owned JsonKeyValueMap back into a serde_json::Map.
//   input:  value - owned JsonKeyValueMap.
//   output: serde_json::Map parsed from the internal JSON string.
//   sideEffects: none
    fn from(value: JsonKeyValueMap) -> Self {
        (&value).into()
    }
    // from:end
}

impl Default for JsonKeyValueMap {
    // default:start
//   purpose: Create a JsonKeyValueMap wrapping an empty JSON object.
//   input:  none.
//   output: JsonKeyValueMap wrapping "{}".
//   sideEffects: none
    fn default() -> Self {
        serde_json::Map::<String, serde_json::Value>::default().into()
    }
    // default:end
}

impl PartialEq for JsonKeyValueMap {
    // eq:start
//   purpose: Compare two JsonKeyValueMaps by deserializing to serde_json::Map and comparing.
//   input:  self, other - JsonKeyValueMap instances.
//   output: bool - true if the JSON maps are semantically equal.
//   sideEffects: none
    fn eq(&self, other: &Self) -> bool {
        let left: serde_json::Map<String, serde_json::Value> = self.into();
        let right: serde_json::Map<String, serde_json::Value> = other.into();
        left == right
    }
    // eq:end
}

impl Eq for JsonKeyValueMap {}

impl JsonKeyValueMap {
    // into_serde_map:start
//   purpose: Convert the JsonKeyValueMap into a serde_json::Map by deserializing its internal string.
//   input:  &self.
//   output: serde_json::Map<String, serde_json::Value>.
//   sideEffects: none
    pub fn into_serde_map(&self) -> serde_json::Map<String, serde_json::Value> {
        self.into()
    }
    // into_serde_map:end
}
