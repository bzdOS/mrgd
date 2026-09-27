// START_AI_HEADER
// MODULE: matrix-hs/src/alias_relinquish_test.rs
// PURPOSE: Unit tests for AppState::mark_alias_relinquished's canonical_alias
//          clearing (see its doc comment) — closes the smaller half of the
//          username/alias asymmetry: username loss gets a renamed map + client
//          discovery; alias loss got nothing beyond the directory entry itself
//          until now. mark_alias_relinquished additionally clears a room's own
//          m.room.canonical_alias state if it still names the just-lost alias.
// DEPENDENCIES: matrix_hs::{AppState, StateEvent}
// END_AI_HEADER

#[cfg(test)]
mod tests {
    use crate::state::{AppState, StateEvent};

    fn seed_alias_and_canonical(state: &AppState, alias: &str, room_id: &str) {
        state
            .aliases
            .lock()
            .expect("aliases lock")
            .insert(alias.to_string(), room_id.to_string());
        let mut rs = state.room_state.lock().expect("room_state lock");
        rs.entry(room_id.to_string()).or_default().push(StateEvent {
            event_type: "m.room.canonical_alias".to_string(),
            state_key: "".to_string(),
            sender: "@alice:localhost".to_string(),
            content: serde_json::json!({ "alias": alias }),
            event_id: "$canon_seed".to_string(),
            room_id: room_id.to_string(),
            origin_server_ts: 0,
        });
    }

    fn canonical_alias_content(state: &AppState, room_id: &str) -> Option<serde_json::Value> {
        let rs = state.room_state.lock().expect("room_state lock");
        rs.get(room_id)?
            .iter()
            .find(|ev| ev.event_type == "m.room.canonical_alias" && ev.state_key.is_empty())
            .map(|ev| ev.content.clone())
    }

    // test:relinquish_clears_matching_canonical_alias:start
    //   purpose: When the room's canonical_alias still names the alias that was
    //            just lost, mark_alias_relinquished must clear it (content -> {}).
    //   input:  room with canonical_alias == "#lost:localhost"; relinquish "#lost:localhost"
    //   output: canonical_alias content becomes {}
    //   sideEffects: none beyond the call itself
    // test:relinquish_clears_matching_canonical_alias:end
    #[tokio::test]
    async fn relinquish_clears_matching_canonical_alias() {
        let state = AppState::new();
        seed_alias_and_canonical(&state, "#lost:localhost", "!room1:localhost");

        state
            .mark_alias_relinquished("#lost:localhost", "@bob:other-node")
            .expect("mark_alias_relinquished");

        assert_eq!(
            canonical_alias_content(&state, "!room1:localhost"),
            Some(serde_json::json!({})),
            "canonical_alias naming the lost alias must be cleared"
        );
        assert!(
            !state.aliases.lock().unwrap().contains_key("#lost:localhost"),
            "the directory entry itself must still be removed, as before"
        );
    }

    // test:relinquish_leaves_different_canonical_alias_untouched:start
    //   purpose: A room whose canonical_alias names a DIFFERENT, still-valid alias
    //            must not be touched when some OTHER alias is relinquished — the
    //            content-match guard, not just "a canonical_alias event exists."
    //   input:  room's canonical_alias == "#kept:localhost"; relinquish "#other:localhost"
    //   output: canonical_alias content unchanged
    //   sideEffects: none
    // test:relinquish_leaves_different_canonical_alias_untouched:end
    #[tokio::test]
    async fn relinquish_leaves_different_canonical_alias_untouched() {
        let state = AppState::new();
        seed_alias_and_canonical(&state, "#kept:localhost", "!room1:localhost");
        state
            .aliases
            .lock()
            .unwrap()
            .insert("#other:localhost".to_string(), "!room1:localhost".to_string());

        state
            .mark_alias_relinquished("#other:localhost", "@bob:other-node")
            .expect("mark_alias_relinquished");

        assert_eq!(
            canonical_alias_content(&state, "!room1:localhost"),
            Some(serde_json::json!({ "alias": "#kept:localhost" })),
            "an unrelated, still-valid canonical_alias must be left untouched"
        );
    }

    // test:relinquish_with_no_local_alias_entry_is_a_noop:start
    //   purpose: Relinquishing an alias this node never held locally (winner-side
    //            node, or already relinquished) must not error and must not touch
    //            any room's state.
    //   input:  empty AppState; relinquish "#never:localhost"
    //   output: Ok(())
    //   sideEffects: none
    // test:relinquish_with_no_local_alias_entry_is_a_noop:end
    #[tokio::test]
    async fn relinquish_with_no_local_alias_entry_is_a_noop() {
        let state = AppState::new();
        state
            .mark_alias_relinquished("#never:localhost", "@bob:other-node")
            .expect("mark_alias_relinquished must not error on an absent alias");
    }
}
