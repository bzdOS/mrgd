// START_AI_HEADER
// MODULE: matrix-hs/src/substrate/keyexpr.rs
// PURPOSE: One encoder/decoder for a room_id used as a Zenoh key-expression segment.
//
//          WHY THIS EXISTS: a room_id is opaque and may legally contain `#`, `?`, `*`
//          and `/`, all of which are FORBIDDEN in a Zenoh key expression (the router
//          answers `Invalid Key Expr`). The stand hit exactly that: a room with `#` in
//          its localpart made every per-room `subscribe` fail, and since a fresh
//          `initial /sync` walks all rooms, the whole sync failed with M_UNKNOWN —
//          while already-established sessions kept working, because an incremental sync
//          never touches the per-room subscriber.
//
//          WHAT IT DOES: percent-encodes every byte outside `[A-Za-z0-9-_]`, so a room
//          id becomes exactly ONE key segment whatever it contains. `%` itself is
//          encoded like any other byte, so the mapping is single-pass and cannot
//          double-encode. Decoding is the exact inverse, which is what lets a node read
//          a room id back out of an incoming key and compare it against its own map.
//
//          WHERE IT MUST BE APPLIED — both ends of every key, or the keys diverge:
//            * the request side  (query_keys) builds `<prefix>/<room>/<leaf>`;
//            * the subscribe side (sink_for) builds the per-room prefix;
//            * the reply side    (the queryables) answers with `<prefix>/<room>/<leaf>`;
//            * the parse side    (room_from_key) must DECODE, so the comparison against
//              local room ids is between two decoded strings.
//
// DEPENDENCIES: none (std only)
// PUBLIC_API: encode_segment, decode_segment
// END_AI_HEADER

/// Percent-encode one key segment: bytes outside `[A-Za-z0-9-_]` become `%XX`.
///
/// Single pass over the bytes, so a literal `%` in the input becomes `%25` and is never
/// mistaken for the start of an escape. Uppercase hex, because that is the canonical
/// spelling and it keeps the mapping byte-for-byte comparable across nodes.
pub fn encode_segment(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for b in raw.as_bytes() {
        let keep = b.is_ascii_alphanumeric() || *b == b'-' || *b == b'_';
        if keep {
            out.push(*b as char);
        } else {
            out.push('%');
            out.push_str(&format!("{b:02X}"));
        }
    }
    out
}

/// Exact inverse of [`encode_segment`]. Invalid escapes are left as written rather than
/// dropped: a key this node did not write must not silently become some other room.
pub fn decode_segment(seg: &str) -> String {
    let bytes = seg.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = &seg[i + 1..i + 3];
            if let Ok(v) = u8::from_str_radix(hex, 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    // round_trips_forbidden_bytes:start
    //   purpose: The whole point of the encoder — a room_id may contain exactly the
    //            bytes Zenoh forbids, and it must survive a round trip unchanged so the
    //            parse side compares like with like. Includes `%` itself, which is the
    //            case a naive "escape the specials" implementation gets wrong.
    //   input:  none
    //   output: ()
    //   sideEffects: none
    // round_trips_forbidden_bytes:end
    #[test]
    fn round_trips_forbidden_bytes() {
        for raw in [
            "!plain:localhost",
            "!#seed433-chatlong:localhost",
            "!chat?long:localhost",
            "!star*room:localhost",
            "!pct%25room:localhost",
            "!slash/inside:localhost",
            "!#?%*/:localhost",
            "!*?%#",
            "",
        ] {
            let enc = encode_segment(raw);
            assert_eq!(decode_segment(&enc), raw, "round trip failed for {raw:?} -> {enc:?}");
            assert!(!enc.contains('#'), "#{raw:?} survived encoding: {enc:?}");
            assert!(!enc.contains('?'), "? survived encoding: {enc:?}");
            assert!(!enc.contains('*'), "* survived encoding: {enc:?}");
            assert!(!enc.contains('/'), "/ survived encoding: {enc:?}");
        }
    }

    // encoding_is_single_segment:start
    //   purpose: A room_id with a `/` in it must not be able to forge a deeper key
    //            path (`<room>/history`), because the router would read it as two
    //            segments. This is why `/` is encoded rather than passed through.
    //   input:  none
    //   output: ()
    //   sideEffects: none
    // encoding_is_single_segment:end
    #[test]
    fn encoding_is_single_segment() {
        let enc = encode_segment("!a/b:localhost");
        // `!` and `:` are encoded too: the spec for this encoder is "everything outside
        // [A-Za-z0-9-_]", and a narrower allow-list would make the key spelling depend
        // on which characters a room happened to get.
        assert_eq!(enc, "%21a%2Fb%3Alocalhost");
        assert_eq!(enc.matches('/').count(), 0);
        assert_eq!(decode_segment(&enc), "!a/b:localhost");
    }

    // percent_is_escaped_not_double_encoded:start
    //   purpose: Encoding twice must not equal encoding once — otherwise a key that
    //            passes through two hops (publish on A, re-publish on B) would grow a
    //            second layer of escapes and the two ends would no longer match.
    //   input:  none
    //   output: ()
    //   sideEffects: none
    // percent_is_escaped_not_double_encoded:end
    #[test]
    fn percent_is_escaped_not_double_encoded() {
        assert_eq!(encode_segment("%"), "%25");
        assert_eq!(encode_segment("%2F"), "%252F");
        assert_eq!(decode_segment("%252F"), "%2F");
        assert_ne!(encode_segment("%2F"), encode_segment("/"));
        // …and yet both decode to themselves' originals only via their own single pass.
        assert_eq!(decode_segment(&encode_segment("%2F")), "%2F");
        assert_eq!(decode_segment(&encode_segment("/")), "/");
    }

    // key_spelling_changes_for_every_room:start
    //   purpose: The encoder rewrites the key for EVERY room, not only for the broken
    //            ones: `!` and `:` are outside the allow-list, so even `!plain:localhost`
    //            moves from `!plain:localhost` to `%21plain%3Alocalhost`. That is a
    //            deliberate consequence of one encoder for all rooms (no second spelling
    //            to reason about), and it is a ROLLOUT fact rather than a detail: a node
    //            on the old build publishes to keys a node on the new build does not
    //            subscribe to. Recorded here so nobody discovers it from a symptom.
    //   input:  none
    //   output: ()
    //   sideEffects: none
    // key_spelling_changes_for_every_room:end
    #[test]
    fn key_spelling_changes_for_every_room() {
        assert_eq!(encode_segment("!plain:localhost"), "%21plain%3Alocalhost");
        // …and the wildcard that answers catch-up still matches, because the encoded id
        // is exactly ONE segment.
        assert!(!encode_segment("!plain:localhost").contains('/'));
    }

    // decode_leaves_unknown_escapes_alone:start
    //   purpose: A key written by someone else may carry an escape this node does not
    //            understand. Dropping or mangling it would silently answer a query about
    //            the wrong room, so an invalid escape stays as written.
    //   input:  none
    //   output: ()
    //   sideEffects: none
    // decode_leaves_unknown_escapes_alone:end
    #[test]
    fn decode_leaves_unknown_escapes_alone() {
        assert_eq!(decode_segment("%zz"), "%zz");
        assert_eq!(decode_segment("%2"), "%2");
        assert_eq!(decode_segment("100%"), "100%");
    }
}

// ── Zenoh acceptance: the test that reproduces the incident ───────────────────
#[cfg(all(test, feature = "cluster"))]
mod zenoh_acceptance {
    use super::encode_segment;

    /// The key the router rejected in the stand incident, and the encoded one.
    const RAW: &str = "mrgd/matrix/room/!#seed433-chatlong:localhost/**";

    fn accepts(expr: &str) -> bool {
        zenoh::key_expr::OwnedKeyExpr::try_from(expr).is_ok()
    }

    // raw_room_id_is_rejected_by_the_router:start
    //   purpose: Reproduce the incident as a test. The stand's rooms with `#` in the
    //            localpart produced `Invalid Key Expr … !#seed433-chatlong:localhost/**`
    //            from declare_subscriber, which failed every per-room subscribe and with
    //            it the whole initial /sync. If zenoh ever stops rejecting this, the test
    //            says so rather than letting the encoder look unnecessary.
    //   input:  none
    //   output: ()
    //   sideEffects: none — pure key-expression parse, no session, no network
    // raw_room_id_is_rejected_by_the_router:end
    #[test]
    fn raw_room_id_is_rejected_by_the_router() {
        assert!(
            !accepts(RAW),
            "zenoh now accepts a raw `#` in a key expression — re-check whether the encoder is still needed"
        );
    }

    // encoded_room_id_is_accepted_by_the_router:start
    //   purpose: The other half of the same proof: what sink_for actually builds for
    //            such a room must parse. This is the assertion that would have caught
    //            the incident, because it runs the real expression through the real
    //            parser instead of eyeballing it.
    //   input:  none
    //   output: ()
    //   sideEffects: none
    // encoded_room_id_is_accepted_by_the_router:end
    #[test]
    fn encoded_room_id_is_accepted_by_the_router() {
        let expr = format!(
            "mrgd/matrix/room/{}/**",
            encode_segment("!#seed433-chatlong:localhost")
        );
        assert!(
            accepts(&expr),
            "the encoded subscribe expression must parse: {expr}"
        );
        // And the catch-up side: request and reply keys for the same room.
        assert!(accepts(&format!(
            "mrgd/matrix/room/{}/history",
            encode_segment("!#seed433-chatlong:localhost")
        )));
        assert!(accepts(&format!(
            "mrgd/matrix/room/{}/state",
            encode_segment("!chat?long:localhost")
        )));
        // A room id with a slash must not be able to forge extra key depth.
        assert!(accepts(&format!(
            "mrgd/matrix/room/{}/**",
            encode_segment("!a/b:localhost")
        )));
    }
}
