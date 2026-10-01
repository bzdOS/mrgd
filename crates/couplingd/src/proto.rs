// START_AI_HEADER
// MODULE: couplingd/src/proto.rs
// PURPOSE: Text control-protocol parser for couplingd.
//          Implements the CMD ARG\n → +OK …\n / -ERR …\n text wire format
//          (SPEC_coupling_v1 §3: SESSION/LOCK/KV/QPUSH/QPOP/QPEEK/SEM/SVC/MEMBERS).
// INTENT: Central parse+dispatch layer — all new verbs are added here so callers
//         (main.rs accept loop) stay verb-agnostic.
// DEPENDENCIES: std
// PUBLIC_API: Cmd, parse_line, fmt_ok, fmt_err
// END_AI_HEADER

use std::fmt;

/// All control-plane verbs understood by couplingd (SPEC_coupling_v1 §3).
// Cmd:start
//   purpose: Represent every parsed control-plane verb + its arguments.
//   input:  produced by parse_line(); consumed by the dispatch loop in main.rs
//   output: enum variants carry only &str slices into the original line buffer
//   sideEffects: none — pure value
// Cmd:end
#[derive(Debug, PartialEq)]
pub enum Cmd<'a> {
    // SESSION OPEN|KEEPALIVE|CLOSE [sid=<u64>] [ttl=<ms>]
    Session { verb: &'a str, args: &'a str },

    // LOCK ACQ|REL <key> [mode=shared|exclusive] [sid=<u64>]
    Lock { verb: &'a str, key: &'a str, args: &'a str },

    // KV GET|CAS|PUT|WATCH <key> [val=<hex>] [ver=<u64>] [fence=<u64>]
    Kv { verb: &'a str, key: &'a str, args: &'a str },

    // QPUSH|QPOP|QPEEK <queue> [payload=<hex>]
    Queue { verb: &'a str, queue: &'a str, args: &'a str },

    // SEM ACQ|REL <name> [sid=<u64>]
    Sem { verb: &'a str, name: &'a str, args: &'a str },

    // SVC REG|RESOLVE <name> [node=<u64>] [sid=<u64>]
    Svc { verb: &'a str, name: &'a str, args: &'a str },

    // MEMBERS — list live nodes in the raft group
    Members,

    // PING — liveness probe
    Ping,

    // CRDT GET|MERGE <key> [delta=<b64>]
    // GET  — read current PnCounter value for key (returns val=<i64>)
    // MERGE — apply a base64-encoded PnCounterDelta to key (idempotent join)
    Crdt { verb: &'a str, key: &'a str, args: &'a str },
}

/// Wire-format error (unknown verb, missing args, etc.).
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("empty command")]
    Empty,
    #[error("unknown verb: {0}")]
    UnknownVerb(String),
    #[error("missing argument for {0}")]
    MissingArg(&'static str),
}

// parse_line:start
//   purpose: Parse one text line (without trailing \n) into a Cmd variant.
//            Splits on whitespace; first token = primary verb, second = sub-verb,
//            third = primary key/name, remainder = trailing args string.
//   input:  line — borrowed &str (caller owns the buffer; lifetime tied to output)
//   output: Result<Cmd<'_>, ParseError>
//   sideEffects: none
// parse_line:end
pub fn parse_line(line: &str) -> Result<Cmd<'_>, ParseError> {
    let line = line.trim();
    if line.is_empty() {
        return Err(ParseError::Empty);
    }

    // Split into at most 4 parts: [VERB] [SUB] [KEY] [rest…]
    let mut parts = line.splitn(4, char::is_whitespace);
    let verb = parts.next().unwrap_or("");
    let sub  = parts.next().unwrap_or("");
    let key  = parts.next().unwrap_or("");
    let rest = parts.next().unwrap_or("");

    match verb {
        "SESSION" => {
            if sub.is_empty() {
                return Err(ParseError::MissingArg("SESSION"));
            }
            // sub = OPEN|KEEPALIVE|CLOSE; key+rest form the args tail
            let args = if key.is_empty() {
                rest
            } else {
                // re-join key + rest as the args string (we only need the raw tail)
                line.splitn(3, char::is_whitespace).nth(2).unwrap_or("")
            };
            Ok(Cmd::Session { verb: sub, args })
        }

        "LOCK" => {
            if sub.is_empty() { return Err(ParseError::MissingArg("LOCK sub-verb")); }
            if key.is_empty() { return Err(ParseError::MissingArg("LOCK key")); }
            Ok(Cmd::Lock { verb: sub, key, args: rest })
        }

        "KV" => {
            if sub.is_empty() { return Err(ParseError::MissingArg("KV sub-verb")); }
            if key.is_empty() { return Err(ParseError::MissingArg("KV key")); }
            Ok(Cmd::Kv { verb: sub, key, args: rest })
        }

        "QPUSH" | "QPOP" | "QPEEK" => {
            if sub.is_empty() { return Err(ParseError::MissingArg("queue name")); }
            // sub = queue name; key+rest = args
            let args = line.splitn(3, char::is_whitespace).nth(2).unwrap_or("");
            Ok(Cmd::Queue { verb, queue: sub, args })
        }

        "SEM" => {
            if sub.is_empty() { return Err(ParseError::MissingArg("SEM sub-verb")); }
            if key.is_empty() { return Err(ParseError::MissingArg("SEM name")); }
            Ok(Cmd::Sem { verb: sub, name: key, args: rest })
        }

        "SVC" => {
            if sub.is_empty() { return Err(ParseError::MissingArg("SVC sub-verb")); }
            if key.is_empty() { return Err(ParseError::MissingArg("SVC name")); }
            Ok(Cmd::Svc { verb: sub, name: key, args: rest })
        }

        "MEMBERS" => Ok(Cmd::Members),
        "PING"    => Ok(Cmd::Ping),

        "CRDT" => {
            if sub.is_empty() { return Err(ParseError::MissingArg("CRDT sub-verb")); }
            if key.is_empty() { return Err(ParseError::MissingArg("CRDT key")); }
            Ok(Cmd::Crdt { verb: sub, key, args: rest })
        }

        other => Err(ParseError::UnknownVerb(other.to_string())),
    }
}

// fmt_ok:start
//   purpose: Format a success response: "+OK <payload>\n".
//   input:  payload — human-readable response body (e.g. "sid=1234 fence=5")
//   output: String — wire-ready response line
//   sideEffects: none
// fmt_ok:end
pub fn fmt_ok(payload: impl fmt::Display) -> String {
    format!("+OK {payload}\n")
}

// fmt_err:start
//   purpose: Format an error response: "-ERR <msg>\n".
//   input:  msg — error description
//   output: String — wire-ready response line
//   sideEffects: none
// fmt_err:end
pub fn fmt_err(msg: impl fmt::Display) -> String {
    format!("-ERR {msg}\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_session_open() {
        let cmd = parse_line("SESSION OPEN ttl=5000").unwrap();
        assert!(matches!(cmd, Cmd::Session { verb: "OPEN", args: "ttl=5000" }));
    }

    #[test]
    fn test_parse_lock_acq() {
        let cmd = parse_line("LOCK ACQ /db/primary mode=exclusive sid=42").unwrap();
        assert!(matches!(cmd, Cmd::Lock { verb: "ACQ", key: "/db/primary", .. }));
    }

    #[test]
    fn test_parse_kv_get() {
        let cmd = parse_line("KV GET /config/port").unwrap();
        assert!(matches!(cmd, Cmd::Kv { verb: "GET", key: "/config/port", args: "" }));
    }

    #[test]
    fn test_parse_members() {
        let cmd = parse_line("MEMBERS").unwrap();
        assert_eq!(cmd, Cmd::Members);
    }

    #[test]
    fn test_parse_unknown() {
        let err = parse_line("FOOBAZ bar").unwrap_err();
        assert!(matches!(err, ParseError::UnknownVerb(_)));
    }

    #[test]
    fn test_fmt_ok() {
        assert_eq!(fmt_ok("sid=1"), "+OK sid=1\n");
    }

    #[test]
    fn test_fmt_err() {
        assert_eq!(fmt_err("bad"), "-ERR bad\n");
    }

    #[test]
    fn test_parse_crdt_get() {
        let cmd = parse_line("CRDT GET /counters/hits").unwrap();
        assert!(matches!(cmd, Cmd::Crdt { verb: "GET", key: "/counters/hits", args: "" }));
    }

    #[test]
    fn test_parse_crdt_merge() {
        let cmd = parse_line("CRDT MERGE /counters/hits delta=AAEC").unwrap();
        assert!(matches!(cmd, Cmd::Crdt { verb: "MERGE", key: "/counters/hits", .. }));
    }
}
