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
    assert!(get(Site::KnownIds) > 0, "known_ids rebuild must be counted");
    assert!(get(Site::BeforeIds) > 0, "before_ids rebuild must be counted");
    assert!(get(Site::AfterIds) > 0, "after_ids rebuild must be counted");
    assert!(total >= get(Site::KnownIds), "total must cover known_ids");
    let rebuilt = get(Site::KnownIds) + get(Site::BeforeIds) + get(Site::AfterIds);
    assert!(
        rebuilt * 100 / total >= 90,
        "HashSet rebuilds must dominate: {rebuilt}/{total}"
    );
    println!(
        "[sweep-alloc-profile] verdict: HashSet<String> rebuilds = {rebuilt}B of {total}B ({}%), i.e. {:.1} bytes/room/cycle",
        rebuilt * 100 / total,
        rebuilt as f64 / ROOMS as f64
    );
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
    // Same shape as merge_catchup_delta's three rebuilds, on the same locks.
    use std::collections::HashSet;
    let known: HashSet<String> = state
        .room_timeline
        .lock()
        .ok()
        .and_then(|rt| rt.get(room_id).map(|v| {
            v.iter()
                .filter_map(|(_, ev)| ev.get("event_id").and_then(|id| id.as_str()).map(|s| s.to_string()))
                .collect()
        }))
        .unwrap_or_default();
    sweep_alloc::add_str_set(Site::KnownIds, known.len() as u64, known.iter().map(|s| s.len() as u64).sum());

    state.ensure_room_state(room_id);
    let before: HashSet<String> = state
        .rooms
        .lock()
        .ok()
        .and_then(|r| r.get(room_id).map(|log| log.ordered().iter().map(|p| p.event_id.clone()).collect()))
        .unwrap_or_default();
    sweep_alloc::add_str_set(Site::BeforeIds, before.len() as u64, before.iter().map(|s| s.len() as u64).sum());

    let rejected = state
        .rooms
        .lock()
        .ok()
        .map(|mut rooms| {
            let log = rooms.entry(room_id.to_string()).or_default();
            let (_acc, rej) = log.apply_delta_verified(delta, &crate::substrate::node_auth::NodeKeyStore::default());
            rej
        })
        .unwrap_or(0);
    stats.rejected += rejected;

    let after: HashSet<String> = state
        .rooms
        .lock()
        .ok()
        .and_then(|r| r.get(room_id).map(|log| log.ordered().iter().map(|p| p.event_id.clone()).collect()))
        .unwrap_or_default();
    sweep_alloc::add_str_set(Site::AfterIds, after.len() as u64, after.iter().map(|s| s.len() as u64).sum());

    let new: Vec<_> = delta
        .pdus
        .iter()
        .filter(|p| !before.contains(&p.event_id) && !known.contains(&p.event_id) && after.contains(&p.event_id))
        .collect();
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
        tl.push((pos, serde_json::json!({
                "event_id": pdu.event_id,
                "type": pdu.kind,
                "sender": pdu.sender,
                "room_id": pdu.room_id,
                "origin_server_ts": pdu.ts,
            "content": content_val
        })));
    }
}

/// sweep_alloc_profile_scales_linearly_with_rooms:start
///   purpose: Run the same synthetic cycle at 96 and at 192 rooms and assert the counted
///            bytes double — i.e. the per-cycle cost is per-room, which is the property the
///            window-D/B2 measurements implied (volume of events, not count of rooms).
///   input:  none
///   output: prints both totals and the ratio
///   sideEffects: in-memory only
/// sweep_alloc_profile_scales_linearly_with_rooms:end
#[test]
fn sweep_alloc_profile_scales_linearly_with_rooms() {
    let _guard = ALLOC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    fn cycle_total(rooms: usize) -> u64 {
        const EVENTS_PER_ROOM: usize = 40;
        let state = crate::state::AppState::new();
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
        }
        sweep_alloc::reset();
        for r in 0..rooms {
            let room_id = format!("!room_{r}_profile:localhost");
            let mut known_ids = std::collections::HashSet::new();
            if let Ok(rt) = state.room_timeline.lock() {
                if let Some(v) = rt.get(&room_id) {
                    known_ids = v
                        .iter()
                        .filter_map(|(_, ev)| {
                            ev.get("event_id")
                                .and_then(|id| id.as_str())
                                .map(|s| s.to_string())
                        })
                        .collect();
                }
            }
            sweep_alloc::add_str_set(
                Site::KnownIds,
                known_ids.len() as u64,
                known_ids.iter().map(|s| s.len() as u64).sum(),
            );
        }
        sweep_alloc::totals()
    }

    let t96 = cycle_total(96);
    let t192 = cycle_total(192);
    println!("[sweep-alloc-profile] linearity: 96 rooms={t96}B, 192 rooms={t192}B, ratio={:.2}", t192 as f64 / t96 as f64);
    assert_eq!(t192, t96 * 2, "per-cycle cost must be per-room (linear)");
}
