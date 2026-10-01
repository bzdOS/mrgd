// START_AI_HEADER
// MODULE: couplingd/src/jailspec.rs
// PURPOSE: Parser for coupling.* declarative jail parameters (SPEC_coupling_v1 §13C).
//          Reads key=value lines from a jail.conf-like text block and produces a
//          CouplingJail descriptor.  couplingd's reconcile-loop consumes this to
//          know which jails it must elect/start/stop/fence without any sidecar.
// INTENT: M1 slice [a] — reconcile-loop + coupling.* parser (wrap without sidecars).
//         Pure Rust, no OS calls — compiles and tests on host (Linux/macOS).
// DEPENDENCIES: std, thiserror
// PUBLIC_API: JailRole, CouplingJail, ParseError, parse_jail_spec, parse_block
// END_AI_HEADER

use std::collections::HashMap;
use thiserror::Error;

// ── Role ─────────────────────────────────────────────────────────────────────

// JailRole:start
//   purpose: Declare the HA role of a jail, determining how couplingd treats it.
//            singleton — exactly one primary at a time; reconcile holds lock:<svc>
//              and starts/stops the jail based on lock ownership (HA-by-failover).
//            worker    — stateless; can run N copies concurrently; no lock contention.
//            crdt      — CRDT-native; write-anywhere; no coordination needed (Ярус-3).
//   input:  parsed from the string value of "coupling.role = ..."
//   output: JailRole variant
//   sideEffects: none
// JailRole:end
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JailRole {
    /// Exactly one instance primary at a time.  Reconcile competes for lock:<svc>.
    Singleton,
    /// Stateless replicas; all can run concurrently without coordination.
    Worker,
    /// CRDT-native app; write-anywhere; Zenoh delta-sync (Ярус 3, deferred).
    Crdt,
}

impl std::fmt::Display for JailRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JailRole::Singleton => f.write_str("singleton"),
            JailRole::Worker    => f.write_str("worker"),
            JailRole::Crdt      => f.write_str("crdt"),
        }
    }
}

impl std::str::FromStr for JailRole {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            "singleton" => Ok(JailRole::Singleton),
            "worker"    => Ok(JailRole::Worker),
            "crdt"      => Ok(JailRole::Crdt),
            other       => Err(ParseError::UnknownRole(other.to_string())),
        }
    }
}

// ── Descriptor ───────────────────────────────────────────────────────────────

// CouplingJail:start
//   purpose: Complete descriptor for a coupling-enabled jail, produced by parse_jail_spec().
//            Consumed by reconcile.rs to know which service to lock on, which dataset
//            to fence, and which hooks to call on role transitions.
//   input:  constructed by parse_jail_spec / parse_block from jail.conf-like text
//   output: used by ReconcileLoop.run() for election + lifecycle management
//   sideEffects: none (pure data)
// CouplingJail:end
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CouplingJail {
    /// Jail name (from `name = ...` key or supplied externally).
    pub name: String,

    /// HA role: how couplingd manages this jail (§13C).
    pub role: JailRole,

    /// Service name to register in SvcStore after winning the election.
    /// Used as the lock key: `lock:<svc>`.  E.g. "pg-matrix".
    pub svc: String,

    /// ZFS dataset path for coupling-VFS fencing (optional; empty string = no dataset).
    /// Example: "tank/coupling/pg-matrix".
    pub dataset: String,

    /// Shell command exec'd inside the jail on role promotion to primary.
    /// E.g. "pg_ctl promote".  Empty string = no hook.
    pub on_promote: String,

    /// Shell command exec'd inside the jail on role demotion (loss of lock).
    /// E.g. "pg_ctl stop -m fast".  Empty string = no hook.
    pub on_demote: String,
}

// ── Errors ───────────────────────────────────────────────────────────────────

/// Errors produced when parsing coupling.* parameters.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ParseError {
    #[error("unknown role '{0}'; expected singleton|worker|crdt")]
    UnknownRole(String),

    #[error("required field '{0}' is missing")]
    MissingField(String),

    #[error("unknown coupling.* key '{0}'")]
    UnknownKey(String),
}

// ── Parser ───────────────────────────────────────────────────────────────────

// parse_jail_spec:start
//   purpose: Parse a flat list of "key = value" lines into a CouplingJail descriptor.
//            Lines that do not start with "coupling." are silently ignored, allowing
//            the parser to run over a complete jail.conf stanza.
//            Comment lines (starting with '#' or '//' after trimming) are skipped.
//            Required fields: coupling.role, coupling.svc.
//            Optional: coupling.dataset (default ""), coupling.on_promote / coupling.on_demote
//            (default ""), name.
//
//            Format (jail.conf subset):
//              name           = "pg-matrix";
//              coupling.role  = singleton;
//              coupling.svc   = pg-matrix;
//              coupling.dataset  = tank/coupling/pg-matrix;
//              coupling.on_promote  = "pg_ctl promote";
//              coupling.on_demote   = "pg_ctl stop -m fast";
//
//   input:  lines — an iterator of &str lines from the jail stanza;
//           jail_name — fallback name when no "name = ..." line is present
//   output: Result<CouplingJail, ParseError>
//   sideEffects: none
// parse_jail_spec:end
pub fn parse_jail_spec<'a>(
    lines:     impl Iterator<Item = &'a str>,
    jail_name: &str,
) -> Result<CouplingJail, ParseError> {
    let mut kv: HashMap<String, String> = HashMap::new();

    for raw in lines {
        let line = raw.trim().trim_end_matches(';').trim();

        // Skip blanks and comments.
        if line.is_empty() || line.starts_with('#') || line.starts_with("//") {
            continue;
        }

        // Split on the first '='.
        let Some((k, v)) = line.split_once('=') else { continue };
        let key = k.trim().to_string();
        let val = strip_quotes(v.trim()).to_string();

        // Accept "name" directly; reject unknown non-coupling.* keys silently
        // (they belong to the jail.conf stanza, not coupling).
        if key == "name" || key.starts_with("coupling.") {
            kv.insert(key, val);
        }
    }

    // Derive name: explicit "name" key → fallback to jail_name argument.
    let name = kv.remove("name")
        .filter(|n| !n.is_empty())
        .unwrap_or_else(|| jail_name.to_string());

    // Required: coupling.role
    let role_str = kv.remove("coupling.role")
        .ok_or_else(|| ParseError::MissingField("coupling.role".to_string()))?;
    let role: JailRole = role_str.parse()?;

    // Required: coupling.svc
    let svc = kv.remove("coupling.svc")
        .ok_or_else(|| ParseError::MissingField("coupling.svc".to_string()))?;

    // Optional fields.
    let dataset    = kv.remove("coupling.dataset").unwrap_or_default();
    let on_promote = kv.remove("coupling.on_promote").unwrap_or_default();
    let on_demote  = kv.remove("coupling.on_demote").unwrap_or_default();

    // Reject leftover unknown coupling.* keys to surface typos early.
    for unknown in kv.keys().filter(|k| k.starts_with("coupling.")) {
        return Err(ParseError::UnknownKey(unknown.clone()));
    }

    Ok(CouplingJail { name, role, svc, dataset, on_promote, on_demote })
}

// parse_block:start
//   purpose: Convenience wrapper — split a multi-line string on newlines and
//            forward to parse_jail_spec.  Useful in tests and for config loaded
//            from a file as a single &str.
//   input:  block — full text of a jail stanza; jail_name — fallback jail name
//   output: Result<CouplingJail, ParseError>
//   sideEffects: none
// parse_block:end
pub fn parse_block(block: &str, jail_name: &str) -> Result<CouplingJail, ParseError> {
    parse_jail_spec(block.lines(), jail_name)
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/// Strip surrounding double-quotes from a value string if present.
fn strip_quotes(s: &str) -> &str {
    if s.len() >= 2 && s.starts_with('"') && s.ends_with('"') {
        &s[1..s.len() - 1]
    } else {
        s
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // parse_minimal:start
    //   purpose: Minimal valid spec (role + svc) parses without error.
    //   input:  two coupling.* lines
    //   output: CouplingJail with role=Singleton, svc="pg-matrix", empty optionals
    //   sideEffects: none
    // parse_minimal:end
    #[test]
    fn parse_minimal() {
        let spec = r#"
            coupling.role = singleton
            coupling.svc  = pg-matrix
        "#;
        let j = parse_block(spec, "pg-matrix-jail").expect("minimal parse must succeed");
        assert_eq!(j.role, JailRole::Singleton);
        assert_eq!(j.svc,  "pg-matrix");
        assert_eq!(j.name, "pg-matrix-jail");
        assert!(j.dataset.is_empty());
        assert!(j.on_promote.is_empty());
        assert!(j.on_demote.is_empty());
    }

    // parse_full:start
    //   purpose: Full spec with all optional fields parses correctly.
    //   input:  complete jail.conf stanza including name, dataset, hooks
    //   output: all fields correctly populated
    //   sideEffects: none
    // parse_full:end
    #[test]
    fn parse_full() {
        let spec = r#"
            name             = "pg-matrix";
            coupling.role    = singleton;
            coupling.svc     = pg-matrix;
            coupling.dataset = tank/coupling/pg-matrix;
            coupling.on_promote = "pg_ctl promote";
            coupling.on_demote  = "pg_ctl stop -m fast";
            path = /jails/pg-matrix;
        "#;
        let j = parse_block(spec, "fallback").expect("full parse must succeed");
        assert_eq!(j.name,       "pg-matrix");
        assert_eq!(j.role,       JailRole::Singleton);
        assert_eq!(j.svc,        "pg-matrix");
        assert_eq!(j.dataset,    "tank/coupling/pg-matrix");
        assert_eq!(j.on_promote, "pg_ctl promote");
        assert_eq!(j.on_demote,  "pg_ctl stop -m fast");
    }

    // parse_worker_role:start
    //   purpose: role=worker is parsed correctly into JailRole::Worker.
    //   input:  spec with coupling.role = worker
    //   output: CouplingJail with role=Worker
    //   sideEffects: none
    // parse_worker_role:end
    #[test]
    fn parse_worker_role() {
        let spec = "coupling.role = worker\ncoupling.svc = synapse";
        let j = parse_block(spec, "synapse-jail").unwrap();
        assert_eq!(j.role, JailRole::Worker);
    }

    // parse_crdt_role:start
    //   purpose: role=crdt is parsed correctly into JailRole::Crdt.
    //   input:  spec with coupling.role = crdt
    //   output: CouplingJail with role=Crdt
    //   sideEffects: none
    // parse_crdt_role:end
    #[test]
    fn parse_crdt_role() {
        let spec = "coupling.role = crdt\ncoupling.svc = presence";
        let j = parse_block(spec, "presence-jail").unwrap();
        assert_eq!(j.role, JailRole::Crdt);
    }

    // parse_missing_role_error:start
    //   purpose: Absence of coupling.role yields MissingField error.
    //   input:  spec with only coupling.svc
    //   output: Err(ParseError::MissingField("coupling.role"))
    //   sideEffects: none
    // parse_missing_role_error:end
    #[test]
    fn parse_missing_role_error() {
        let spec = "coupling.svc = pg-matrix";
        let err = parse_block(spec, "x").unwrap_err();
        assert_eq!(err, ParseError::MissingField("coupling.role".to_string()));
    }

    // parse_missing_svc_error:start
    //   purpose: Absence of coupling.svc yields MissingField error.
    //   input:  spec with only coupling.role
    //   output: Err(ParseError::MissingField("coupling.svc"))
    //   sideEffects: none
    // parse_missing_svc_error:end
    #[test]
    fn parse_missing_svc_error() {
        let spec = "coupling.role = singleton";
        let err = parse_block(spec, "x").unwrap_err();
        assert_eq!(err, ParseError::MissingField("coupling.svc".to_string()));
    }

    // parse_unknown_role_error:start
    //   purpose: An unrecognized role value yields UnknownRole error.
    //   input:  coupling.role = primary (not a valid role)
    //   output: Err(ParseError::UnknownRole("primary"))
    //   sideEffects: none
    // parse_unknown_role_error:end
    #[test]
    fn parse_unknown_role_error() {
        let spec = "coupling.role = primary\ncoupling.svc = x";
        let err = parse_block(spec, "x").unwrap_err();
        assert_eq!(err, ParseError::UnknownRole("primary".to_string()));
    }

    // parse_unknown_coupling_key_rejected:start
    //   purpose: An unrecognized coupling.* key yields UnknownKey error.
    //   input:  coupling.typo = foo alongside valid required keys
    //   output: Err(ParseError::UnknownKey("coupling.typo"))
    //   sideEffects: none
    // parse_unknown_coupling_key_rejected:end
    #[test]
    fn parse_unknown_coupling_key_rejected() {
        let spec = "coupling.role = singleton\ncoupling.svc = x\ncoupling.typo = foo";
        let err = parse_block(spec, "x").unwrap_err();
        assert_eq!(err, ParseError::UnknownKey("coupling.typo".to_string()));
    }

    // parse_comments_ignored:start
    //   purpose: Lines starting with '#' or '//' are skipped (no parse error).
    //   input:  spec with comment lines mixed in
    //   output: parse succeeds; result matches non-comment lines
    //   sideEffects: none
    // parse_comments_ignored:end
    #[test]
    fn parse_comments_ignored() {
        let spec = r#"
            # This is a jail stanza for pg-matrix
            // another comment style
            coupling.role = singleton
            coupling.svc  = pg-matrix
        "#;
        let j = parse_block(spec, "fallback").expect("comments must not cause parse error");
        assert_eq!(j.svc, "pg-matrix");
    }

    // parse_semicolon_stripped:start
    //   purpose: Trailing semicolons (jail.conf style) are stripped from values.
    //   input:  values with trailing semicolons
    //   output: parsed values contain no trailing semicolons
    //   sideEffects: none
    // parse_semicolon_stripped:end
    #[test]
    fn parse_semicolon_stripped() {
        let spec = "coupling.role = singleton;\ncoupling.svc = pg-matrix;";
        let j = parse_block(spec, "x").expect("semicolons must be stripped");
        assert_eq!(j.role, JailRole::Singleton);
        assert_eq!(j.svc,  "pg-matrix");
    }

    // parse_non_coupling_keys_ignored:start
    //   purpose: Jail.conf keys not starting with "coupling." or "name" are silently ignored.
    //   input:  spec with path, ip4, allow.chflags alongside coupling keys
    //   output: parse succeeds; extra keys produce no error
    //   sideEffects: none
    // parse_non_coupling_keys_ignored:end
    #[test]
    fn parse_non_coupling_keys_ignored() {
        let spec = r#"
            path        = /jails/pg;
            ip4         = inherit;
            allow.chflags = 1;
            coupling.role = singleton;
            coupling.svc  = pg-matrix;
        "#;
        let j = parse_block(spec, "pg").expect("non-coupling keys must be ignored");
        assert_eq!(j.svc, "pg-matrix");
    }
}
