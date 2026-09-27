// START_AI_HEADER
// MODULE: mac-companion/zenoh-link-obfs/src/lib.rs
// PURPOSE: DPI-resistant Zenoh unicast transport: AEAD-framed (SS-AEAD style) byte stream over raw TCP, with X25519 + PSK handshake.
// INTENT: Replace TLS-based zenoh-link-tls with a transport that has no TLS ClientHello and thus no JA3/JA4 fingerprint, bypassing passive DPI classification.
// DEPENDENCIES: async_trait, zenoh_config, zenoh_core, zenoh_link_commons, zenoh_protocol, zenoh_result, base64
// PUBLIC_API: PreSharedKey, StaticServerKey, KEY_LEN, MAX_PAYLOAD, LinkManagerUnicastObfs, LinkUnicastObfs, OBFS_LOCATOR_PREFIX, OBFS_PSK_BASE64, OBFS_PSK_ENV, ObfsLocatorInspector, ObfsConfigurator
// END_AI_HEADER

//
// bsdOS — zenoh-link-obfs
//
// A DPI-resistant Zenoh unicast transport: AEAD-framed (SS-AEAD style) byte
// stream over raw TCP, with an X25519 + PSK handshake. No TLS, hence no
// ClientHello and no JA3/JA4 fingerprint. See PLAN-dpi-transport.md.
//
// Public surface mirrors `zenoh-link-tls`'s shape (LinkManager, Configurator,
// LocatorInspector, *_LOCATOR_PREFIX) so it can be wired into Zenoh's
// `LinkManagerBuilderUnicast` the same way the TLS link is.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
use std::str::FromStr;

use async_trait::async_trait;
use zenoh_config::Config as ZenohConfig;
use zenoh_core::zconfigurable;
use zenoh_link_commons::{ConfigurationInspector, LocatorInspector};
use zenoh_protocol::{
    core::{endpoint::Config, Locator, Metadata, Reliability},
    transport::BatchSize,
};
use zenoh_result::{zerror, ZResult};

mod crypto;
mod unicast;

pub use crypto::{PreSharedKey, StaticServerKey, KEY_LEN, MAX_PAYLOAD};
pub use unicast::{LinkManagerUnicastObfs, LinkUnicastObfs};

/// Locator/endpoint prefix for this transport: `obfs/<host:port>`.
pub const OBFS_LOCATOR_PREFIX: &str = "obfs";

/// obfs is a reliable, stream-oriented transport (it runs over TCP).
const IS_RELIABLE: bool = true;

/// Like TLS, obfs is byte-stream oriented; Zenoh encodes payload length in 16 bits,
/// so the MTU is capped at 2^16 - 1.
const OBFS_MAX_MTU: BatchSize = BatchSize::MAX;

/// Per-AEAD-record framing overhead: encrypted 2-byte length (2 + 16 tag) +
/// payload tag (16) = 2 + 16 + 16 = 34 bytes. This covers ONE record only.
///
/// NOTE: this is NOT enough to make a whole Zenoh batch fit in a single record.
/// `BatchSize` is `u16` (≤ 65535) while a single AEAD record carries at most
/// [`MAX_PAYLOAD`] = 0x3FFF (16383) bytes, so any batch larger than `MAX_PAYLOAD`
/// is split by `write_frames` into multiple records (~4 records of 16383 bytes +
/// remainder for a near-MTU batch). That fixed 16383-byte record size is itself a
/// packet-size fingerprint; record padding / length-jitter (PLAN §5, Этап 3) is
/// the planned mitigation. See also `MAX_PAYLOAD` in `crypto.rs`.
pub(crate) const OBFS_FRAME_OVERHEAD: BatchSize = 34;

/// Endpoint config key for supplying the PSK inline as base64 (e.g. for tests).
/// Production deployments should prefer the `BSDOS_OBFS_PSK` environment variable.
pub const OBFS_PSK_BASE64: &str = "obfs_psk_base64";

/// Environment variable carrying the base64-encoded 32-byte PSK.
pub const OBFS_PSK_ENV: &str = "BSDOS_OBFS_PSK";

zconfigurable! {
    static ref OBFS_DEFAULT_MTU: BatchSize = OBFS_MAX_MTU;
    // LINGER timeout (seconds) for graceful shutdown of the underlying TCP stream.
    static ref OBFS_LINGER_TIMEOUT: i32 = 10;
    // Throttle (microseconds) for the accept loop upon error. 100 ms.
    static ref OBFS_ACCEPT_THROTTLE_TIME: u64 = 100_000;
}

/// Inspects locators of the form `obfs/...`.
#[derive(Default, Clone, Copy)]
pub struct ObfsLocatorInspector;

#[async_trait]
impl LocatorInspector for ObfsLocatorInspector {
    // protocol:start
//   purpose: Return the locator prefix for the obfs transport.
//   input:  &self
//   output: &str — the OBFS_LOCATOR_PREFIX constant ("obfs")
//   sideEffects: none
    fn protocol(&self) -> &str {
        OBFS_LOCATOR_PREFIX
    }
    // protocol:end

    // is_multicast:start
//   purpose: Report whether the obfs transport supports multicast for the given locator.
//   input:  _locator: &Locator — the locator to inspect
//   output: ZResult<bool> — always Ok(false)
//   sideEffects: none
    async fn is_multicast(&self, _locator: &Locator) -> ZResult<bool> {
        Ok(false)
    }
    // is_multicast:end

    // is_reliable:start
//   purpose: Determine whether the given locator describes a reliable obfs transport.
//   input:  locator: &Locator — the locator inspected for its RELIABILITY metadata
//   output: ZResult<bool> — true if locator specifies Reliable or no explicit reliability; false otherwise
//   sideEffects: none
    fn is_reliable(&self, locator: &Locator) -> ZResult<bool> {
        if let Some(reliability) = locator
            .metadata()
            .get(Metadata::RELIABILITY)
            .map(Reliability::from_str)
            .transpose()?
        {
            Ok(reliability == Reliability::Reliable)
        } else {
            Ok(IS_RELIABLE)
        }
    }
    // is_reliable:end
}

/// Translates the Zenoh `Config` into endpoint parameters for the obfs link.
///
/// Unlike TLS, the obfs transport has no typed section in `zenoh_config` (it is a
/// custom link). The PSK is supplied out-of-band (env `BSDOS_OBFS_PSK`), so there
/// is nothing in the global config to project onto endpoints — we return an empty
/// parameter string.
///
/// REVIEW: if/when an `obfs` section is added to `zenoh_config`, project its fields
/// here the way `TlsConfigurator::inspect_config` does.
#[derive(Default, Clone, Copy, Debug)]
pub struct ObfsConfigurator;

impl ConfigurationInspector<ZenohConfig> for ObfsConfigurator {
    // inspect_config:start
//   purpose: Translate the Zenoh global configuration into endpoint parameters for the obfs transport.
//   input:  _config: &ZenohConfig — the Zenoh configuration to inspect (unused; PSK is supplied out-of-band)
//   output: ZResult<String> — always Ok("") (empty parameter string)
//   sideEffects: none
    fn inspect_config(&self, _config: &ZenohConfig) -> ZResult<String> {
        Ok(String::new())
    }
    // inspect_config:end
}

/// Load the 32-byte PSK for an endpoint.
///
/// Resolution order:
///   1. endpoint config parameter [`OBFS_PSK_BASE64`] (base64), if present;
///   2. environment variable [`OBFS_PSK_ENV`] (base64).
///
/// The decoded key must be exactly 32 bytes.
// load_psk:start
//   purpose: Resolve the 32-byte PSK from endpoint config parameter or environment variable, in that order.
//   input:  config: &Config<'_> — the endpoint configuration to search for OBFS_PSK_BASE64
//   output: ZResult<PreSharedKey> — the 32-byte pre-shared key, or error if not found or wrong size
//   sideEffects: Reads environment variable BSDOS_OBFS_PSK on second attempt
pub(crate) fn load_psk(config: &Config<'_>) -> ZResult<PreSharedKey> {
    if let Some(b64) = config.get(OBFS_PSK_BASE64) {
        let raw = base64_decode(b64)?;
        return PreSharedKey::from_bytes(&raw);
    }
    match std::env::var(OBFS_PSK_ENV) {
        Ok(b64) => {
            let raw = base64_decode(&b64)?;
            PreSharedKey::from_bytes(&raw)
        }
        Err(_) => Err(zerror!(
            "obfs PSK not found: set endpoint param `{}` or env `{}` (base64, 32 bytes)",
            OBFS_PSK_BASE64,
            OBFS_PSK_ENV
        )
        .into()),
    }
}
// load_psk:end

// base64_decode:start
//   purpose: Decode a base64 string into raw bytes.
//   input:  data: &str — the base64-encoded string
//   output: ZResult<Vec<u8>> — decoded bytes, or error on invalid base64
//   sideEffects: none
fn base64_decode(data: &str) -> ZResult<Vec<u8>> {
    use base64::{engine::general_purpose, Engine};
    general_purpose::STANDARD
        .decode(data.trim())
        .map_err(|e| zerror!("obfs: base64 decode of PSK failed: {e:?}").into())
}
// base64_decode:end
