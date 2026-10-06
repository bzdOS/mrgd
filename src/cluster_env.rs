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
}
