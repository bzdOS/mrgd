// cluster_env.rs — the cluster layer's env decision, as a pure function.
//
// WHY IT IS SEPARATE. Until 05.10 this decision lived inline in build_state(), and the
// whole cluster layer sat behind #[cfg(feature = "cluster")]: a binary built without the
// feature could not answer "is the cluster on here?", because the code that would answer
// was the code that was missing. The stand was lifted from such a binary and came up
// healthy — pid-file, port, versions 200 — with no zenoh listener, no catch-up, and no
// "cluster mode:" line to contradict it. 6 h of soak measured that stand.
//
// What is here is the decision with nothing left to strip: given the four env values, it
// says whether the cluster layer is wanted, in which mode, with which endpoints. It is
// reachable from a unit test whether or not the feature is compiled in, so the env can be
// checked on any build, and a caller that ignored the answer would have to ignore a
// function rather than miss a log line.
//
// The env values arrive through `get`, a lookup closure, not through std::env directly:
// the test then reads the same values the stand reads without mutating the process
// environment, which is global state a parallel test would inherit.
//
// WHAT IT DOES NOT DO. It opens nothing. No session, no socket, no timer — the caller
// applies the plan to a zenoh::Config. That is what lets the regression test for this
// run without a network.

// What the four variables asked for, as the code reads them today.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterEnv {
    pub connect: Option<String>,
    pub listen: Option<String>,
    pub scouting_off: bool,
    pub client_mode: bool,
}

impl ClusterEnv {
    /// Read the four variables with `get`. `MATRIX_HS_ZENOH_SCOUTING` accepts
    /// off/0/false, case- and space-insensitive; `MATRIX_HS_ZENOH_MODE=client` is
    /// case-insensitive. An empty string counts as set for the endpoints, exactly as
    /// `env::var(..).ok()` saw it before this was extracted — a node with
    /// MATRIX_HS_ZENOH_LISTEN="" asked for a cluster, and asking is the part that
    /// the stand's env does.
    pub fn from_lookup<F>(get: F) -> Self
    where
        F: Fn(&str) -> Option<String>,
    {
        Self {
            connect: get("MATRIX_HS_ZENOH_CONNECT"),
            listen: get("MATRIX_HS_ZENOH_LISTEN"),
            scouting_off: get("MATRIX_HS_ZENOH_SCOUTING")
                .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "off" | "0" | "false"))
                .unwrap_or(false),
            client_mode: get("MATRIX_HS_ZENOH_MODE")
                .map(|v| v.trim().eq_ignore_ascii_case("client"))
                .unwrap_or(false),
        }
    }

    /// Read the process environment.
    pub fn from_process_env() -> Self {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    /// Does this env ask for the cluster layer? True for either endpoint, for scouting
    /// turned off, or for client mode — the same four-way condition build_state used.
    pub fn wants_cluster(&self) -> bool {
        self.connect.is_some() || self.listen.is_some() || self.scouting_off || self.client_mode
    }

    /// The line the binary prints when it opens a configured session. It names connect
    /// and listen as Option<String>, so an unset one reads None and a set one reads
    /// Some(".."), which is what makes this string usable as the marker for "this
    /// binary carries the cluster layer".
    pub fn describe(&self, prefix: &str) -> String {
        if self.wants_cluster() {
            format!(
                "cluster mode: connect={:?} listen={:?} mode={} scouting={} prefix={}",
                self.connect,
                self.listen,
                if self.client_mode { "client" } else { "peer" },
                if self.scouting_off { "off" } else { "on" },
                prefix
            )
        } else {
             format!("cluster mode: peer/scouting (no connect/listen env) prefix={prefix}")
         }
     }

    /// The `listen/endpoints` value (JSON5 array) this env asks for. Zenoh's peer
    /// default is `tcp/[::]:0` (zenoh-config defaults.rs) — a listener on every
    /// interface — so an unset or empty MATRIX_HS_ZENOH_LISTEN becomes an explicit
    /// empty list, which zenoh binds nothing for (zenoh orchestrator.rs,
    /// bind_listeners: "Starting with no listener endpoints!"). Such a node only
    /// dials out. A set LISTEN is passed through as written.
    pub fn listen_endpoints_json5(&self) -> String {
        match self.listen.as_deref().map(str::trim) {
            Some(l) if !l.is_empty() => to_json5_endpoints(l),
            _ => "[]".to_string(),
        }
    }

    /// Reject plaintext `tcp/` Zenoh endpoints on non-loopback addresses, and
    /// wildcard listen (0.0.0.0, [::]) under any protocol.
    /// Per the fleet rule (see .env.example: "never plaintext Zenoh on a public
    /// interface"), such a configuration is a security bug, not a transport
    /// choice. Returns Err with the first insecure endpoint found.
    pub fn validate_transport_security(&self) -> Result<(), String> {
        for (label, endpoints) in [
            ("MATRIX_HS_ZENOH_CONNECT", &self.connect),
            ("MATRIX_HS_ZENOH_LISTEN", &self.listen),
        ] {
            if let Some(ep_list) = endpoints {
                for ep in ep_list.split(',') {
                    let ep = ep.trim();
                    if ep.is_empty() {
                        continue;
                    }
                    let host = endpoint_host(ep);
                    if label == "MATRIX_HS_ZENOH_LISTEN" && is_wildcard_host(&host) {
                        return Err(format!(
                            "{label}: endpoint {ep} listens on all interfaces; \
                             name a loopback/private address"
                        ));
                    }
                    if ep.starts_with("tcp/") {
                        if !host.is_empty() && !is_loopback_host(&host) {
                            return Err(format!(
                                "{label}: plaintext tcp/ endpoint on non-loopback address \
                                 {} is forbidden; use obfs/ or TLS instead (see .env.example rule)",
                                host
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// Host part of "proto/host:port#meta": brackets removed for IPv6; an unbracketed
/// address with several colons ("::", "::1:7448") is returned up to `#` as is.
fn endpoint_host(ep: &str) -> String {
    let addr = ep.split_once('/').map_or(ep, |(_, a)| a);
    let addr = addr.split('#').next().unwrap_or("");
    if let Some(rest) = addr.strip_prefix('[') {
        return rest.split(']').next().unwrap_or("").to_string();
    }
    if addr.matches(':').count() > 1 {
        return addr.to_string();
    }
    addr.split(':').next().unwrap_or("").to_string()
}

/// True for the "all interfaces" addresses.
fn is_wildcard_host(host: &str) -> bool {
    matches!(host, "0.0.0.0" | "::" | "::0" | "0:0:0:0:0:0:0:0")
}

/// True for loopback addresses that are safe for plaintext tcp/.
fn is_loopback_host(host: &str) -> bool {
    matches!(host, "127.0.0.1" | "::1" | "localhost") || host.starts_with("::1:")
}

/// "ep1, ep2" -> ["ep1","ep2"], the shape zenoh's JSON5 endpoint setters take.
pub fn to_json5_endpoints(s: &str) -> String {
    let quoted: Vec<String> = s.split(',').map(|e| format!("\"{}\"", e.trim())).collect();
    format!("[{}]", quoted.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_of(pairs: &[(&str, &str)]) -> ClusterEnv {
        let v: Vec<(String, String)> =
            pairs.iter().map(|(k, x)| (k.to_string(), x.to_string())).collect();
        ClusterEnv::from_lookup(|k| v.iter().find(|(a, _)| a == k).map(|(_, b)| b.clone()))
    }

    // The stand's env, as read from its own file on 06.10: a listen endpoint in obfs
    // form, scouting off, no connect. This is the pair of values whose absence in a
    // binary produced a healthy, empty stand.
    fn stand_env() -> ClusterEnv {
        env_of(&[
            ("MATRIX_HS_ZENOH_LISTEN", "obfs/198.51.100.7:7447"),
            ("MATRIX_HS_ZENOH_SCOUTING", "off"),
        ])
    }

    #[test]
    fn stand_env_wants_the_cluster_and_describes_its_listener() {
        let e = stand_env();
        assert!(e.wants_cluster(), "a listen endpoint must bring the cluster layer up");
        assert!(!e.client_mode, "the stand is a peer, not a router client");
        assert!(e.scouting_off, "scouting=off was explicit in the stand's env");
        let line = e.describe("mrgd/matrix/room");
        assert!(
            line.starts_with("cluster mode: connect=None listen=Some("),
            "the marker line must name both endpoints: {line}"
        );
        assert!(line.contains("mode=peer"), "{line}");
        assert!(line.contains("scouting=off"), "{line}");
    }

    #[test]
    fn an_env_without_any_of_the_four_does_not_want_the_cluster() {
        let e = env_of(&[("MATRIX_HS_SERVER_NAME", "localhost")]);
        assert!(!e.wants_cluster());
        assert!(e.describe("p").contains("peer/scouting (no connect/listen env)"));
    }

    #[test]
    fn each_of_the_four_alone_is_enough() {
        for pairs in [
            vec![("MATRIX_HS_ZENOH_CONNECT", "tcp/198.51.100.9:7447")],
            vec![("MATRIX_HS_ZENOH_LISTEN", "tcp/198.51.100.7:7447")],
            vec![("MATRIX_HS_ZENOH_SCOUTING", "off")],
            vec![("MATRIX_HS_ZENOH_MODE", "client")],
        ] {
            let e = env_of(&pairs);
            assert!(e.wants_cluster(), "{pairs:?} alone must ask for the cluster");
        }
    }

    #[test]
    fn scouting_off_spellings_and_mode_case_do_not_matter() {
        for v in ["off", "OFF", " Off ", "0", "false"] {
            assert!(env_of(&[("MATRIX_HS_ZENOH_SCOUTING", v)]).scouting_off, "{v:?}");
        }
        for v in ["on", "yes", "1", "true", ""] {
            assert!(!env_of(&[("MATRIX_HS_ZENOH_SCOUTING", v)]).scouting_off, "{v:?}");
        }
        for v in ["client", "Client", "CLIENT", " client "] {
            assert!(env_of(&[("MATRIX_HS_ZENOH_MODE", v)]).client_mode, "{v:?}");
        }
        assert!(!env_of(&[("MATRIX_HS_ZENOH_MODE", "peer")]).client_mode);
    }

    #[test]
    fn an_empty_endpoint_var_still_asks_for_the_cluster() {
        // env::var(..).ok() sees Some(""), and asking for a cluster with an empty
        // endpoint is a configuration mistake to report at the socket, not a reason to
        // come up silently without the layer.
        let e = env_of(&[("MATRIX_HS_ZENOH_LISTEN", "")]);
        assert!(e.wants_cluster());
        assert!(e.describe("p").contains("listen=Some(\"\")"));
    }

    #[test]
    fn endpoints_become_a_json5_array() {
        assert_eq!(to_json5_endpoints("tcp/a:1"), "[\"tcp/a:1\"]");
        assert_eq!(to_json5_endpoints(" tcp/a:1 , tcp/b:2 "), "[\"tcp/a:1\",\"tcp/b:2\"]");
        assert_eq!(to_json5_endpoints(""), "[\"\"]");
    }

    #[test]
    fn plaintext_tcp_on_loopback_is_allowed() {
        let e = env_of(&[("MATRIX_HS_ZENOH_LISTEN", "tcp/127.0.0.1:7448")]);
        assert!(e.validate_transport_security().is_ok());
        let e = env_of(&[("MATRIX_HS_ZENOH_CONNECT", "tcp/127.0.0.1:7449")]);
        assert!(e.validate_transport_security().is_ok());
        let e = env_of(&[("MATRIX_HS_ZENOH_LISTEN", "tcp/localhost:7448")]);
        assert!(e.validate_transport_security().is_ok());
    }

    #[test]
    fn obfs_on_any_address_is_allowed() {
        let e = env_of(&[(
            "MATRIX_HS_ZENOH_LISTEN",
            "obfs/198.51.100.7:7447#obfs_psk_base64=AAECAw==",
        )]);
        assert!(e.validate_transport_security().is_ok());
        let e = env_of(&[(
            "MATRIX_HS_ZENOH_CONNECT",
            "obfs/198.51.100.9:7447#obfs_psk_base64=AAECAw==",
        )]);
        assert!(e.validate_transport_security().is_ok());
    }

    #[test]
    fn plaintext_tcp_on_public_address_is_rejected() {
        let e = env_of(&[("MATRIX_HS_ZENOH_LISTEN", "tcp/0.0.0.0:7448")]);
        assert!(e.validate_transport_security().is_err());
        let e = env_of(&[("MATRIX_HS_ZENOH_CONNECT", "tcp/198.51.100.9:7447")]);
        assert!(e.validate_transport_security().is_err());
        let msg = e.validate_transport_security().unwrap_err();
        assert!(msg.contains("198.51.100.9"));
    }

    #[test]
    fn mixed_endpoints_with_one_plaintext_public_is_rejected() {
        // loopback OK + public tcp/ in the same var → still rejected
        let e = env_of(&[(
            "MATRIX_HS_ZENOH_LISTEN",
            "tcp/127.0.0.1:7448,tcp/198.51.100.7:7449",
        )]);
        assert!(e.validate_transport_security().is_err());
    }

    #[test]
    fn empty_or_unset_endpoints_are_allowed() {
        let e = env_of(&[("MATRIX_HS_ZENOH_SCOUTING", "off")]);
        assert!(e.validate_transport_security().is_ok());
        let e = env_of(&[("MATRIX_HS_ZENOH_LISTEN", "")]);
        assert!(e.validate_transport_security().is_ok());
    }

    #[test]
    fn unset_or_empty_listen_becomes_an_explicit_empty_list() {
        // Zenoh's peer default is tcp/[::]:0 — all interfaces. "[]" is what keeps it off.
        assert_eq!(env_of(&[]).listen_endpoints_json5(), "[]");
        let e = env_of(&[("MATRIX_HS_ZENOH_CONNECT", "tcp/127.0.0.1:7449")]);
        assert_eq!(e.listen_endpoints_json5(), "[]");
        assert_eq!(env_of(&[("MATRIX_HS_ZENOH_LISTEN", "")]).listen_endpoints_json5(), "[]");
        assert_eq!(env_of(&[("MATRIX_HS_ZENOH_LISTEN", "  ")]).listen_endpoints_json5(), "[]");
    }

    #[test]
    fn explicit_listen_is_passed_through() {
        let e = env_of(&[("MATRIX_HS_ZENOH_LISTEN", "tcp/127.0.0.1:7448")]);
        assert_eq!(e.listen_endpoints_json5(), "[\"tcp/127.0.0.1:7448\"]");
    }

    #[test]
    fn wildcard_listen_is_rejected_under_any_protocol() {
        for ep in [
            "tcp/0.0.0.0:7448",
            "udp/0.0.0.0:7448",
            "tls/0.0.0.0:7448",
            "obfs/0.0.0.0:7447#obfs_psk_base64=AAECAw==",
            "tcp/[::]:7448",
            "udp/[::]:7448",
            "obfs/[::]:7447#obfs_psk_base64=AAECAw==",
            "quic/[::]:7448",
            "tcp/127.0.0.1:7448,obfs/0.0.0.0:7447",
        ] {
            let e = env_of(&[("MATRIX_HS_ZENOH_LISTEN", ep)]);
            let err = e.validate_transport_security().unwrap_err();
            assert!(err.contains("MATRIX_HS_ZENOH_LISTEN"), "{ep}: {err}");
        }
    }

    #[test]
    fn named_interface_listen_and_bracketed_loopback_are_allowed() {
        for ep in [
            "obfs/198.51.100.7:7447#obfs_psk_base64=AAECAw==",
            "udp/198.51.100.7:7448",
            "tcp/[::1]:7448",
            "tcp/::1:7448",
            "udp/127.0.0.1:7448",
        ] {
            let e = env_of(&[("MATRIX_HS_ZENOH_LISTEN", ep)]);
            assert!(e.validate_transport_security().is_ok(), "{ep}");
        }
    }

    #[test]
    fn wildcard_in_connect_is_not_a_listen_error() {
        // CONNECT to 0.0.0.0 is not a listener; only plaintext-tcp rules apply to it.
        let e = env_of(&[("MATRIX_HS_ZENOH_CONNECT", "obfs/0.0.0.0:7447")]);
        assert!(e.validate_transport_security().is_ok());
    }
}
