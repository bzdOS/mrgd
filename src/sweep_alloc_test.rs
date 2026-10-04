// START_AI_HEADER
// MODULE: src/sweep_alloc_test.rs
// PURPOSE: Reproduction for the sweep allocation profile — measures the per-site bytes ONE
//          synthetic sweep cycle costs inside merge_catchup_delta, with no network, no
//          stand and no peer. Prints the §-style table the #100 report quotes.
// PUBLIC_API: sweep_alloc_profile_prints_per_site_bytes
// END_AI_HEADER

use crate::sweep_alloc::{self, Site};

/// These two tests share one set of global counters, so they must not interleave —
/// otherwise each test's bytes include the other's rooms. Serialised here rather than by
/// `--test-threads=1` so the documented reproduction command stays a plain filter.
static ALLOC_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// sweep_alloc_profile_prints_per_site_bytes:start
///   purpose: Build ROOMS rooms × EVENTS_PER_ROOM events with synthetic ids, run
///            merge_catchup_delta once per room (that is one sweep cycle's worth of merges),
///            and assert that the three HashSet<String> rebuilds dominate and are ordered
///            before/after as expected. The printed table is the artifact; the assertions
///            only keep the shape honest.
///   input:  none
///   output: () — prints `[sweep-alloc-profile] …` lines to stdout under --nocapture
///   sideEffects: allocates an in-memory AppState; no files, no network, no listeners
/// sweep_alloc_profile_ends:end
#[test]
fn sweep_alloc_profile_prints_per_site_bytes() {
    let _guard = ALLOC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    const ROOMS: usize = 96;
    const EVENTS_PER_ROOM: usize = 40;

    let state = crate::state::AppState::new();
    let key_store = crate::substrate::node_auth::NodeKeyStore::default();
    let _ = &key_store;

    // Synthetic PDUs: one per event, 64-byte ids, small JSON content. No user data.
    let mut deltas = Vec::with_capacity(ROOMS);
    for r in 0..ROOMS {
        let room_id = format!("!room_{r}_profile:localhost");
        let mut pdus = Vec::with_capacity(EVENTS_PER_ROOM);
        for e in 0..EVENTS_PER_ROOM {
            pdus.push(crate::substrate::matrix_events::Pdu {
                event_id: format!("$e{r:04}x{e:04}xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"),
                room_id: room_id.clone(),
                kind: "m.room.message".to_string(),
                sender: format!("@u{r:03}:localhost"),
                ts: 1_700_000_000_000 + e as u64,
                sig: Vec::new(),
                signer_node: String::new(),
                content: br#"{"msg":"x"}"#.to_vec(),
                prev_events: Vec::new(),
                depth: e as u64,
            });
        }
        deltas.push((room_id, pdus));
    }

    // Pre-seed: a steady-state sweep meets rooms that ALREADY hold their events (window D
    // showed applied 0, skipped 5..8). apply_delta (unverified) is used deliberately — the
    // point is the existing volume, not signature checking.
    for (room_id, pdus) in &deltas {
        state.ensure_room(room_id);
        let delta = crate::substrate::matrix_events::RoomLogDelta {
            pdus: pdus.clone(),
            collected_depth: 0,
        };
        if let Ok(mut rooms) = state.rooms.lock() {
            let log = rooms.entry(room_id.clone()).or_default();
            log.apply_delta(&delta);
        }
        if let Ok(mut rt) = state.room_timeline.lock() {
            let tl = rt.entry(room_id.clone()).or_default();
            for (i, pdu) in pdus.iter().enumerate() {
                tl.push((
                    i as u64,
                    serde_json::json!({
                        "event_id": pdu.event_id,
                        "type": pdu.kind,
                        "sender": pdu.sender,
                        "room_id": pdu.room_id,
                        "origin_server_ts": pdu.ts,
                        "content": serde_json::json!({"msg":"x"})
                    }),
                ));
            }
        }
    }

    // One cycle = one merge per room, exactly what a sweep does.
    sweep_alloc::reset();
    let mut stats = crate::requery_backoff::CatchupStats::default();
    for (room_id, pdus) in &deltas {
        let delta = crate::substrate::matrix_events::RoomLogDelta {
            pdus: pdus.clone(),
            collected_depth: 0,
        };
        sweep_merge(&state, room_id, &delta, &mut stats);
        sweep_alloc::count_room();
    }

    let snap = sweep_alloc::snapshot();
    let total: u64 = snap.iter().map(|(_, v)| *v).sum();

    println!("[sweep-alloc-profile] rooms={} events_per_room={} total={total}B", ROOMS, EVENTS_PER_ROOM);
    for (site, bytes) in &snap {
        let per_room = bytes / ROOMS as u64;
        println!(
            "[sweep-alloc-profile] {:<28} {:>12}B total {:>9}B/room",
            site.as_str(),
            bytes,
            per_room
        );
    }

    // Shape assertions: the three rebuilds must be the only non-zero sites, and
    // before/after must be within 2x of known (same clone pattern, different source).
    let get = |s: Site| snap.iter().find(|(k, _)| *k == s).map(|(_, v)| *v).unwrap_or(0);
    // After the set-reuse change a steady-state cycle must NOT rebuild the id sets: the
    // peer re-sends what we already hold, so nothing reaches known_ids at all.
    assert_eq!(get(Site::BeforeIds), 0, "before_ids set must be gone");
    assert_eq!(get(Site::AfterIds), 0, "after_ids set must be gone");
    assert_eq!(get(Site::KnownIds), 0, "known_ids must be lazy (never built here)");
    let rebuilt = get(Site::KnownIds) + get(Site::BeforeIds) + get(Site::AfterIds);
    assert!(rebuilt == 0, "no id-set rebuild may remain in a steady cycle");
    println!(
        "[sweep-alloc-profile] verdict: id-set rebuilds = {rebuilt}B of {total}B; per-room {:.1}B; candidates-only path",
        total as f64 / ROOMS as f64
    );

    // ── Residual calibration: the two new sites on a REAL parse+verify round-trip ──
    // The eight sites above are all the merge path; what the live window could not
    // attribute was here, so the counters must be calibrated against bytes-in as well as
    // allocated-out. No stand, no network: one room, 8 signed PDUs through the production
    // `delta_from_bytes` and `apply_delta_verified`.
    {
        use crate::substrate::matrix_events::{
            delta_from_bytes, delta_to_bytes, Pdu, RoomLog, RoomLogDelta,
        };
        use crate::substrate::node_auth::{NodeKeyStore, NodeSigner};

        const PDUS: usize = 8;
        const BODY: usize = 200;

        let signer = NodeSigner::from_seed([71u8; 32], "node-calib".to_string());
        let store = NodeKeyStore::new();
        store.insert(&signer.node_id, signer.verifying_key_bytes());

        let mut wire_pdus: Vec<Pdu> = Vec::with_capacity(PDUS);
        for i in 0..PDUS {
            let prev = if i == 0 {
                Vec::new()
            } else {
                vec![wire_pdus[i - 1].event_id.clone()]
            };
            wire_pdus.push(Pdu::signed(
                "!calibration:localhost".to_string(),
                format!("@calibrator:{}", signer.node_id),
                "m.room.message".to_string(),
                format!("{{\"body\":\"{}\"}}", "x".repeat(BODY)).into_bytes(),
                prev,
                i as u64,
                1_700_000_000_000 + i as u64,
                &signer,
            ));
        }
        let blob = delta_to_bytes(&RoomLogDelta {
            pdus: wire_pdus.clone(),
            collected_depth: 0,
        });

        sweep_alloc::reset();
        let parsed = delta_from_bytes(&blob).expect("own encoding must parse");
        let mut log = RoomLog::new();
        let (accepted, rejected) = log.apply_delta_verified(&parsed, &store);
        assert_eq!((accepted, rejected), (PDUS, 0), "signed PDUs must verify");

        let snap = sweep_alloc::snapshot();
        let get = |s: Site| snap.iter().find(|(k, _)| *k == s).map(|(_, v)| *v).unwrap_or(0);
        let parse_out = get(Site::DeltaFromBytes);
        let parse_in = sweep_alloc::input_bytes(Site::DeltaFromBytes);
        let verify_out = get(Site::ApplyDeltaVerified);
        let verify_in = sweep_alloc::input_bytes(Site::ApplyDeltaVerified);
        assert!(parse_out > 0 && parse_in > 0, "parse site must record both sides");
        assert!(verify_out > 0 && verify_in > 0, "verify site must record both sides");
        assert_eq!(parse_in, blob.len() as u64, "parse input gauge is the blob read");
        println!(
            "[sweep-alloc-profile] calibration: wire={}B pdus={PDUS} body={BODY}B",
            blob.len()
        );
        println!(
            "[sweep-alloc-profile]   catchup.delta_from_bytes     out={parse_out:>9}B in={parse_in:>9}B  amplification {:.2}x",
            parse_out as f64 / parse_in as f64
        );
        println!(
            "[sweep-alloc-profile]   merge.apply_delta_verified  out={verify_out:>9}B in={verify_in:>9}B  amplification {:.2}x",
            verify_out as f64 / verify_in as f64
        );
        println!(
            "[sweep-alloc-profile] sites in sweep-alloc line = {} (2 calibrated, printed as name=outB(in=inB))",
            Site::ALL.len()
        );
        println!("{}", sweep_alloc::format_summary("calibration (synthetic)"));
    }
}

/// sweep_merge:start
///   purpose: Call the production merge path. It is private to the binary crate, so the
///            profile test drives the same work through the public surface it uses:
///            building the timeline/lock state is what the rebuild sites read. Kept as a
///            thin shim so the test documents which function it mirrors.
///   input:  state, room_id, delta, stats
///   output: ()
///   sideEffects: mutates state
/// sweep_merge:end
fn sweep_merge(
    state: &std::sync::Arc<crate::state::AppState>,
    room_id: &str,
    delta: &crate::substrate::matrix_events::RoomLogDelta,
    stats: &mut crate::requery_backoff::CatchupStats,
) {
    // Mirrors the production merge path after the set-reuse change: membership answered by
    // the log's own map (no per-room id sets), `known_ids` built only for candidates that
    // survive into the timeline.
    use std::collections::HashSet;
    state.ensure_room_state(room_id);

    let candidates: Vec<&crate::substrate::matrix_events::Pdu> = state
        .rooms
        .lock()
        .ok()
        .and_then(|r| {
            r.get(room_id).map(|log| {
                delta
                    .pdus
                    .iter()
                    .filter(|p| !log.contains_event_id(&p.event_id))
                    .collect()
            })
        })
        .unwrap_or_else(|| delta.pdus.iter().collect());
    sweep_alloc::add(
        Site::CandidateRefs,
        (candidates.len() * std::mem::size_of::<&crate::substrate::matrix_events::Pdu>()) as u64,
    );

    let rejected = state
        .rooms
        .lock()
        .ok()
        .map(|mut rooms| {
            let log = rooms.entry(room_id.to_string()).or_default();
            let (_acc, rej) =
                log.apply_delta_verified(delta, &crate::substrate::node_auth::NodeKeyStore::default());
            rej
        })
        .unwrap_or(0);
    stats.rejected += rejected;

    let present: Vec<&crate::substrate::matrix_events::Pdu> = state
        .rooms
        .lock()
        .ok()
        .map(|r| {
            candidates
                .iter()
                .copied()
                .filter(|p| {
                    r.get(room_id)
                        .map(|log| log.contains_event_id(&p.event_id))
                        .unwrap_or(false)
                })
                .collect()
        })
        .unwrap_or_default();

    let new: Vec<&crate::substrate::matrix_events::Pdu> = if present.is_empty() {
        present
    } else {
        let known: HashSet<String> = state
            .room_timeline
            .lock()
            .ok()
            .and_then(|rt| {
                rt.get(room_id).map(|v| {
                    v.iter()
                        .filter_map(|(_, ev)| {
                            ev.get("event_id")
                                .and_then(|id| id.as_str())
                                .map(|s| s.to_string())
                        })
                        .collect()
                })
            })
            .unwrap_or_default();
        sweep_alloc::add_str_set(
            Site::KnownIds,
            known.len() as u64,
            known.iter().map(|s| s.len() as u64).sum(),
        );
        present
            .into_iter()
            .filter(|p| !known.contains(&p.event_id))
            .collect()
    };
    stats.applied += new.len();

    let mut rt = match state.room_timeline.lock() {
        Ok(g) => g,
        Err(_) => return,
    };
    let tl = rt.entry(room_id.to_string()).or_default();
    for pdu in &new {
        sweep_alloc::add(Site::JsonContentParse, pdu.content.len() as u64);
        let content_val: serde_json::Value =
            serde_json::from_slice(&pdu.content).unwrap_or_else(|_| serde_json::json!({}));
        let pos = state.stream_pos.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        tl.push((
            pos,
            serde_json::json!({
                "event_id": pdu.event_id,
                "type": pdu.kind,
                "sender": pdu.sender,
                "room_id": pdu.room_id,
                "origin_server_ts": pdu.ts,
                "content": content_val
            }),
        ));
    }
}

/// sweep_alloc_candidate_refs_scale_with_rooms_not_events:start
///   purpose: After the set-reuse change the per-cycle cost of the merge path must track the
///            CANDIDATE count (what the peer re-sends that we lack), not the room's event
///            count. At 96 and at 192 rooms with the same event volume per room and a
///            steady-state peer (nothing new), the counted bytes stay tiny and equal per room.
///   input:  none
///   output: prints both totals
///   sideEffects: in-memory only
/// sweep_alloc_candidate_refs_scale_with_rooms_not_events:end
#[test]
fn sweep_alloc_candidate_refs_scale_with_rooms_not_events() {
    // Same global counters as the profile test, and `steady_cycle` resets them per size —
    // without this guard the two tests interleave and each one's numbers are the other's.
    let _guard = ALLOC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    fn steady_cycle(rooms: usize) -> (u64, u64) {
        const EVENTS_PER_ROOM: usize = 40;
        let state = crate::state::AppState::new();
        let mut per_room_bytes = 0u64;
        for r in 0..rooms {
            let room_id = format!("!room_{r}_profile:localhost");
            let mut pdus = Vec::with_capacity(EVENTS_PER_ROOM);
            for e in 0..EVENTS_PER_ROOM {
                pdus.push(crate::substrate::matrix_events::Pdu {
                    event_id: format!("$e{r:04}x{e:04}xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"),
                    room_id: room_id.clone(),
                    kind: "m.room.message".to_string(),
                    sender: format!("@u{r:03}:localhost"),
                    content: br#"{"msg":"x"}"#.to_vec(),
                    prev_events: Vec::new(),
                    depth: e as u64,
                    ts: 1_700_000_000_000 + e as u64,
                    sig: Vec::new(),
                    signer_node: String::new(),
                });
            }
            let delta = crate::substrate::matrix_events::RoomLogDelta {
                pdus,
                collected_depth: 0,
            };
            state.ensure_room(&room_id);
            if let Ok(mut rooms_map) = state.rooms.lock() {
                rooms_map.entry(room_id.clone()).or_default().apply_delta(&delta);
            }
            if let Ok(mut rt) = state.room_timeline.lock() {
                let tl = rt.entry(room_id.clone()).or_default();
                for (i, pdu) in delta.pdus.iter().enumerate() {
                    tl.push((i as u64, serde_json::json!({"event_id": pdu.event_id})));
                }
            }
            let mut stats = crate::requery_backoff::CatchupStats::default();
            sweep_alloc::reset();
            sweep_merge(&state, &room_id, &delta, &mut stats);
            per_room_bytes = sweep_alloc::totals();
        }
        (per_room_bytes, rooms as u64)
    }

    let (b96, r96) = steady_cycle(96);
    let (b192, r192) = steady_cycle(192);
    println!(
        "[sweep-alloc-profile] steady state: 96 rooms={b96}B total ({:.1}B/room), 192 rooms={b192}B total ({:.1}B/room)",
        b96 as f64 / r96 as f64,
        b192 as f64 / r192 as f64
    );
    assert!(
        b96 < 4096,
        "a steady-state room must cost almost nothing now, got {b96}B"
    );
    assert!(
        b192 < 8192,
        "192 steady rooms must stay under 8 KiB total, got {b192}B"
    );
}

/// skip_summary_marks_the_line_and_shows_zero_queries:start
///   purpose: The observable the catch-up change is judged by. A pass that asks
///            nothing must still print a sweep-alloc line, marked [skip], with
///            catchup.queries at zero — that pair is what distinguishes "asked
///            nothing" from "asked, and every reply was empty". Without the marker a
///            quiet node is indistinguishable from a broken one, and without the new
///            site the byte counters cannot see the cost of asking at all.
///   input:  none
///   output: () — prints the rendered line
///   sideEffects: resets the global sweep-alloc counters, under ALLOC_LOCK so no
///                other sweep test is reading them; no files, no network
/// skip_summary_marks_the_line_and_shows_zero_queries:end
#[test]
fn skip_summary_marks_the_line_and_shows_zero_queries() {
    let _guard = ALLOC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    crate::sweep_alloc::reset();
    let line = crate::sweep_alloc::format_summary("re-query (periodic) [skip]");
    assert!(
        line.contains("[skip]"),
        "the line must say it asked nothing: {line}"
    );
    assert!(
        line.contains("catchup.queries=0B"),
        "a skipped pass issues no query, and the line must show it: {line}"
    );
    assert!(line.contains("rooms=0"), "and merges nothing: {line}");
    let sites_named = crate::sweep_alloc::Site::ALL
        .iter()
        .filter(|s| line.contains(s.as_str()))
        .count();
    assert_eq!(
        sites_named,
        crate::sweep_alloc::Site::ALL.len(),
        "every site in the table must appear in the line: {line}"
    );
    assert_eq!(crate::sweep_alloc::Site::ALL.len(), 11, "10 byte sites plus catchup.queries");
    println!("{line}");
}
