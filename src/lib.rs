// START_AI_HEADER
// MODULE: src/lib.rs
// PURPOSE: Matrix CS-API Stage 2 skeleton.
//          Thin HTTP layer (axum) over an in-memory CRDT event-store (RoomLog from
//          `substrate`, this crate's protocol-agnostic coordination-free replication
//          core — see substrate/mod.rs). `substrate` has no axum dependency and must
//          never gain one; it is a module boundary, not a crate boundary, kept purely
//          for engineering hygiene now that matrix-hs is this crate's only consumer
//          (see ARCHITECTURE-boundaries.md for the history of that split).
//          Stage 1: push/pull round-trip through the CRDT store.
//          Stage 2: extended routes for Element handshake (discovery, account, room state,
//                   stubs for keys/voip/push, incremental sync with since tokens, long-poll).
//          Stage 3 OTK: keys/upload + keys/claim (OWNERSHIP-PARTITION exactly-once barrier).
//          Media repository: routes/media.rs — upload/download/thumbnail/config under
//          both /_matrix/media/v3 and /_matrix/client/v1/media (local-node storage;
//          cross-node fetch is a documented, tested, deferred seam — see that module).
// DEPENDENCIES: axum 0.8, tokio, serde_json, substrate::matrix_events::{Pdu, RoomLog}, image (jpeg+png)
// PUBLIC_API: AppState, router, substrate
// END_AI_HEADER

pub mod auth;
pub mod error;
pub mod hubd_bridge;
pub mod hub_replic;
pub mod persist;
pub mod requery_backoff;
pub mod routes;
pub mod scripting;
pub mod substrate;
pub mod state;

// test_util:start
//   purpose: Shared test-only helpers. ZENOH_TEST_LOCK serializes every test that opens a
//            real zenoh::Session: since the 2026-08 crate merge, all `#[cfg(test)]` modules
//            (formerly split across the `mrgd` and `matrix-hs` crates, run as two separate
//            test binaries) now live in ONE test binary. Rust's test harness runs tests in
//            that binary concurrently (default = num_cpus threads), so ~13 tests that each
//            open a real peer-mode Zenoh session (multicast scouting on loopback) now race
//            each other in the same process — this reliably hung/stalled past 5 minutes
//            under default parallelism, and passed in ~100s with `--test-threads=1`.
//            Acquiring this semaphore for a test's full body forces those ~13 tests to run
//            one at a time while leaving the other ~delta (pure in-memory, no network) free to
//            run in parallel as before.
//   input:  none
//   output: a permit; drop it (end of scope) to release
//   sideEffects: none beyond serializing callers
// test_util:end
// Gated on `cluster` as well as `test`: every caller is a cluster-only test, so in a
// default-feature test build the static has no users and trips dead_code.
#[cfg(all(test, feature = "cluster"))]
pub(crate) mod test_util {
    use std::sync::atomic::{AtomicU32, Ordering};

    pub(crate) static ZENOH_TEST_LOCK: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);

    // unique_prefix:start
    //   purpose: A key prefix that cannot collide with any other Zenoh participant —
    //            not the deployed node on this host (bsdos/*), not another run of this
    //            suite left behind, not a second consumer of the default namespace.
    //            Content-addressed dedup means a collision would not corrupt data, but
    //            a wildcard discovery subscriber or catch-up queryable on a shared
    //            prefix WOULD pull strangers' rooms into a test's assertions.
    //   input:  a human-readable base, e.g. "mrgd/matrix/room/cluster-test-0"
    //   output: base + "/run<pid>-<n>", n unique within this process
    //   sideEffects: bumps a process-local counter
    // unique_prefix:end
    pub(crate) fn unique_prefix(base: &str) -> String {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("{base}/run{}-{}", std::process::id(), n)
    }

    // open_mesh:start
    //   purpose: N Zenoh sessions that form a PRIVATE mesh: every session listens on
    //            its own ephemeral loopback port and connects to the ports of the
    //            sessions opened before it (full mesh, no transit-routing assumptions),
    //            with multicast AND gossip scouting disabled so no participant outside
    //            this call can join or be discovered. Replaces
    //            `zenoh::open(Config::default())`, which relied on loopback multicast
    //            scouting — and therefore merged the test mesh with whatever else was
    //            scouting on the host (deployed matrix-hs node, the hubd queue
    //            router), making ~4 cluster tests fail per run with a shifting set.
    //            Explicit listen/connect is also what production does; tests now
    //            exercise the same carrier shape.
    //   input:  N, inferred from the destructuring pattern
    //   output: N linked sessions, scouting off, on unique loopback ports
    //   sideEffects: binds loopback ports for the sessions' lifetime
    // open_mesh:end
    pub(crate) async fn open_mesh<const N: usize>() -> [zenoh::Session; N] {
        let mut addrs: Vec<String> = Vec::with_capacity(N);
        let mut sessions: Vec<zenoh::Session> = Vec::with_capacity(N);
        for _ in 0..N {
            // Probe a free port by binding and immediately releasing it. The window
            // before zenoh re-binds is negligible: every caller holds ZENOH_TEST_LOCK,
            // so nothing else in this process races for it.
            let port = std::net::TcpListener::bind("127.0.0.1:0")
                .expect("probe loopback port")
                .local_addr()
                .expect("loopback addr")
                .port();
            let addr = format!("tcp/127.0.0.1:{port}");
            let mut cfg = zenoh::Config::default();
            cfg.insert_json5("listen/endpoints", &format!("[\"{addr}\"]"))
                .expect("zenoh listen config");
            if !addrs.is_empty() {
                let quoted: Vec<String> = addrs.iter().map(|a| format!("\"{a}\"")).collect();
                cfg.insert_json5("connect/endpoints", &format!("[{}]", quoted.join(",")))
                    .expect("zenoh connect config");
            }
            cfg.insert_json5("scouting/multicast/enabled", "false")
                .expect("zenoh scouting config");
            cfg.insert_json5("scouting/gossip/enabled", "false")
                .expect("zenoh gossip config");
            sessions.push(zenoh::open(cfg).await.expect("isolated zenoh session"));
            addrs.push(addr);
        }
        sessions
            .try_into()
            .unwrap_or_else(|_| panic!("open_mesh: internal size error"))
    }
}

#[cfg(test)]
mod account_data_test;
#[cfg(test)]
mod account_password_test;
#[cfg(test)]
mod as_socket_test;
#[cfg(test)]
mod alias_relinquish_test;
#[cfg(all(test, feature = "cluster"))]
mod cluster_test;
#[cfg(test)]
mod createroom_extras_test;
#[cfg(all(test, feature = "cluster"))]
mod device_lists_cluster_test;
#[cfg(test)]
mod device_lists_test;
#[cfg(test)]
mod element_handshake_test;
#[cfg(test)]
mod encryption_test;
#[cfg(all(test, feature = "cluster"))]
mod ephemeral_cluster_test;
#[cfg(test)]
mod ephemeral_test;
#[cfg(test)]
mod hubd_bridge_test;
#[cfg(all(test, feature = "cluster"))]
mod hub_replic_test;
#[cfg(all(test, feature = "cluster"))]
mod keys_cluster_test;
#[cfg(all(test, feature = "cluster"))]
mod two_pool_demo_test;
#[cfg(test)]
mod keys_test;
#[cfg(test)]
mod logout_test;
#[cfg(test)]
mod media_test;
#[cfg(test)]
mod membership_test;
#[cfg(test)]
mod messages_test;
#[cfg(test)]
mod persist_test;
#[cfg(test)]
mod timeline_limited_signal_test;
#[cfg(test)]
mod push_test;
#[cfg(test)]
mod redact_test;
#[cfg(test)]
mod register_test;
#[cfg(test)]
mod rename_test;
#[cfg(test)]
mod room_keys_test;
#[cfg(test)]
mod rooms_test;
#[cfg(test)]
mod scripting_test;
#[cfg(test)]
mod sliding_sync_test;
#[cfg(all(test, feature = "cluster"))]
mod state_replication_cluster_test;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod user_directory_test;
#[cfg(test)]
mod voip_test;

pub use state::AppState;

use axum::extract::DefaultBodyLimit;
use axum::Router;
use std::sync::Arc;

// router:start
//   purpose: Build the axum Router wiring all CS-API endpoints onto the shared AppState.
//            All HTTP deps are confined to this crate; mrgd has no axum dependency.
//   input:  state — Arc<AppState> shared across all handlers
//   output: Router — ready to bind and serve
//   sideEffects: none (pure construction)
// router:end
pub fn router(state: Arc<AppState>) -> Router {
    use axum::routing::{get, post};

    // Axum caps request bodies at 2 MiB by default, and that cap is enforced
    // BEFORE any handler runs — so post_upload's own max_media_upload_bytes()
    // check never saw an oversized body, and the server advertised
    // `m.upload.size` = 50 MiB in /media/v3/config while silently refusing
    // anything past 2 MiB. Found in production: a camera pipeline whose JPEG
    // snapshots (under 2 MiB) all succeeded while every MP4 clip (3-8 MiB)
    // failed with a bare 413, 94 of them in three hours.
    //
    // The headroom above the advertised limit is deliberate. With the layer set
    // exactly at the limit, axum rejects an oversized upload itself and the
    // client gets a 413 with no body — but the spec wants M_TOO_LARGE, which
    // only post_upload can produce. One spare MiB lets a normal overshoot reach
    // the handler and get the proper error, while still bounding what a peer
    // can make this process buffer.
    const UPLOAD_LIMIT_HEADROOM: u64 = 1024 * 1024;
    let upload_body_limit = usize::try_from(
        state
            .max_media_upload_bytes()
            .saturating_add(UPLOAD_LIMIT_HEADROOM),
    )
    .unwrap_or(usize::MAX);

    Router::new()
        // ── Well-known discovery ──────────────────────────────────────────────
        .route(
            "/.well-known/matrix/client",
            get(routes::discovery::get_well_known),
        )
        // ── Versions ─────────────────────────────────────────────────────────
        .route(
            "/_matrix/client/versions",
            get(routes::versions::get_versions),
        )
        // ── CS-API mounted under BOTH prefixes ────────────────────────────────
        //   v3 = current spec name (Element, modern clients).
        //   r0 = legacy spec name, still emitted by matrix-nio / matrix-commander
        //        and many older clients. r0 and v3 are wire-identical for these
        //        endpoints, so we serve one route table under both mount points.
        .nest("/_matrix/client/v3", cs_api_routes())
        .route(
            "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync",
            post(routes::sliding_sync::post_sliding_sync),
        )
        .route(
            "/_matrix/client/v1/sync",
            post(routes::sliding_sync::post_sliding_sync),
        )
        .nest("/_matrix/client/r0", cs_api_routes())
        // ── Media repository ──────────────────────────────────────────────────
        //   Authenticated v1 client-media paths + legacy /_matrix/media/v3 paths
        //   share the same handlers (see routes/media.rs module header).
        .route(
            "/_matrix/client/v1/media/upload",
            post(routes::media::post_upload).layer(DefaultBodyLimit::max(upload_body_limit)),
        )
        .route(
            "/_matrix/client/v1/media/download/{server_name}/{media_id}",
            get(routes::media::get_download),
        )
        .route(
            "/_matrix/client/v1/media/thumbnail/{server_name}/{media_id}",
            get(routes::media::get_thumbnail),
        )
        .route(
            "/_matrix/client/v1/media/config",
            get(routes::media::get_config),
        )
        .route(
            "/_matrix/media/v3/upload",
            post(routes::media::post_upload).layer(DefaultBodyLimit::max(upload_body_limit)),
        )
        .route(
            "/_matrix/media/v3/download/{server_name}/{media_id}",
            get(routes::media::get_download),
        )
        .route(
            "/_matrix/media/v3/thumbnail/{server_name}/{media_id}",
            get(routes::media::get_thumbnail),
        )
        .route("/_matrix/media/v3/config", get(routes::media::get_config))
        .fallback(fallback_unrecognized)
        .with_state(state)
}

// fallback_unrecognized:start
//   purpose: Handle any request that matched no route. axum's own default 404 has
//            an EMPTY body — but every Matrix response, including errors, MUST be a
//            JSON object with errcode/error (spec requirement). A bare empty body on
//            an unmatched endpoint (e.g. a client probing GET /_matrix/client/v1/
//            auth_metadata to check for OIDC support, which this server does not
//            implement) can crash strict client-side JSON error parsers instead of
//            letting them gracefully fall back to legacy login — reproduced live
//            with FluffyChat (Dart matrix SDK): the empty 404 on auth_metadata surfaced
//            as an unhandled "Oops, something went wrong" instead of silently
//            continuing to password login.
//   input:  none (catches every unmatched method+path)
//   output: 404 {"errcode":"M_UNRECOGNIZED","error":"Unrecognized request"}
//   sideEffects: none
// fallback_unrecognized:end
async fn fallback_unrecognized() -> impl axum::response::IntoResponse {
    (
        axum::http::StatusCode::NOT_FOUND,
        axum::Json(serde_json::json!({
            "errcode": "M_UNRECOGNIZED",
            "error": "Unrecognized request"
        })),
    )
}

// cs_api_routes:start
//   purpose: Build the prefix-relative CS-API route table, mounted under both
//            /_matrix/client/v3 and /_matrix/client/r0 (wire-identical prefixes).
//   input:  none
//   output: Router<Arc<AppState>> — state applied by the caller via with_state.
//   sideEffects: none (pure construction)
// cs_api_routes:end
fn cs_api_routes() -> Router<Arc<AppState>> {
    use axum::routing::{get, post, put};

    Router::new()
        // ── Login: GET lists flows, POST authenticates ────────────────────────
        .route(
            "/login",
            get(routes::discovery::get_login_flows).post(routes::login::post_login),
        )
        // ── Create room ───────────────────────────────────────────────────────
        .route("/createRoom", post(routes::rooms::post_create_room))
        // ── Send message event → Pdu → RoomLog.add ───────────────────────────
        .route(
            "/rooms/{room_id}/send/{event_type}/{txn_id}",
            put(routes::send::put_send_event),
        )
        // ── Sync ──────────────────────────────────────────────────────────────
        .route("/sync", get(routes::sync::get_sync))
        // ── Account ───────────────────────────────────────────────────────────
        .route("/account/whoami", get(routes::account::get_whoami))
        .route(
            "/account/deactivate",
            post(routes::account::post_deactivate),
        )
        .route("/logout", post(routes::account::post_logout))
        .route("/logout/all", post(routes::account::post_logout_all))
        .route(
            "/user_directory/search",
            post(routes::account::post_user_directory_search),
        )
        .route(
            "/account/password",
            post(routes::account_password::post_change_password),
        )
        .route("/capabilities", get(routes::account::get_capabilities))
        .route("/pushrules/", get(routes::account::get_pushrules))
        .route(
            "/user/{user_id}/filter",
            post(routes::account::post_user_filter),
        )
        .route(
            "/user/{user_id}/filter/{filter_id}",
            get(routes::account::get_user_filter),
        )
        .route("/joined_rooms", get(routes::account::get_joined_rooms))
        .route("/profile/{user_id}", get(routes::account::get_profile))
        .route("/devices", get(routes::account::get_devices))
        // ── Room join & directory ─────────────────────────────────────────────
        .route(
            "/join/{room_id_or_alias}",
            post(routes::room_state::post_join_room_or_alias),
        )
        .route(
            "/rooms/{room_id}/join",
            post(routes::room_state::post_join_room),
        )
        .route(
            "/directory/room/{room_alias}",
            get(routes::room_state::get_directory_room),
        )
        // ── Room membership: leave / invite / kick / ban / unban / forget ─────
        .route(
            "/rooms/{room_id}/leave",
            post(routes::room_state::post_leave_room),
        )
        .route(
            "/rooms/{room_id}/invite",
            post(routes::room_state::post_invite_room),
        )
        .route(
            "/rooms/{room_id}/kick",
            post(routes::room_state::post_kick_room),
        )
        .route(
            "/rooms/{room_id}/ban",
            post(routes::room_state::post_ban_room),
        )
        .route(
            "/rooms/{room_id}/unban",
            post(routes::room_state::post_unban_room),
        )
        .route(
            "/rooms/{room_id}/forget",
            post(routes::room_state::post_forget_room),
        )
        // ── Room state read/write ─────────────────────────────────────────────
        .route(
            "/rooms/{room_id}/state",
            get(routes::room_state::get_room_state),
        )
        // Empty-state_key form (no trailing segment) — see
        // get_room_state_event_empty_key's contract comment: matchit does not
        // match an empty final path segment to a {param} capture, so state_key=""
        // events (m.room.create, m.room.name, m.room.encryption, ...) must be
        // reachable via this 2-segment route rather than a trailing slash.
        .route(
            "/rooms/{room_id}/state/{event_type}",
            get(routes::room_state::get_room_state_event_empty_key)
                .put(routes::room_state::put_room_state_event_empty_key),
        )
        // Same form WITH the trailing slash. matrix-rust-sdk (the backend of
        // FluffyChat/Element-X, and BareChat via FFI) builds the empty-state_key
        // state GET as ".../state/{event_type}/" — a literal trailing slash —
        // which matches NEITHER the 2-segment route above NOR the 3-segment
        // one below. Found live 2026-08-24 driving BareChat's real SDK against
        // this server: the pre-send m.room.encryption probe 404'd with
        // M_UNRECOGNIZED and surfaced as every sendMessage failing.
        .route(
            "/rooms/{room_id}/state/{event_type}/",
            get(routes::room_state::get_room_state_event_empty_key)
                .put(routes::room_state::put_room_state_event_empty_key),
        )
        .route(
            "/rooms/{room_id}/state/{event_type}/{state_key}",
            get(routes::room_state::get_room_state_event)
                .put(routes::room_state::put_room_state_event),
        )
        .route(
            "/rooms/{room_id}/members",
            get(routes::room_state::get_room_members),
        )
        .route(
            "/rooms/{room_id}/joined_members",
            get(routes::room_state::get_joined_members),
        )
        .route(
            "/rooms/{room_id}/messages",
            get(routes::room_state::get_room_messages),
        )
        .route(
            "/rooms/{room_id}/redact/{event_id}/{txn_id}",
            put(routes::redact::put_redact_event),
        )
        // ── Registration ──────────────────────────────────────────────────────
        .route("/register", post(routes::register::post_register))
        .route(
            "/register/available",
            get(routes::register::get_register_available),
        )
        // ── E2EE key endpoints (OWNERSHIP-PARTITION OTK barrier) ─────────────
        .route("/keys/upload", post(routes::keys::post_keys_upload))
        .route("/keys/query", post(routes::keys::post_keys_query))
        .route("/keys/claim", post(routes::keys::post_keys_claim))
        // ── Cross-signing (MVP — see routes/keys.rs post_device_signing_upload
        //    for the UIA seam note) ─────────────────────────────────────────
        .route(
            "/keys/device_signing/upload",
            post(routes::keys::post_device_signing_upload),
        )
        .route(
            "/keys/signatures/upload",
            post(routes::keys::post_signatures_upload),
        )
        // ── E2EE key backup (/room_keys) — single-node, per-user storage ────
        .route(
            "/room_keys/version",
            get(routes::room_keys::get_room_keys_version_current)
                .post(routes::room_keys::post_room_keys_version),
        )
        .route(
            "/room_keys/version/{version}",
            get(routes::room_keys::get_room_keys_version)
                .put(routes::room_keys::put_room_keys_version)
                .delete(routes::room_keys::delete_room_keys_version),
        )
        .route(
            "/room_keys/keys",
            get(routes::room_keys::get_room_keys_all)
                .put(routes::room_keys::put_room_keys_all)
                .delete(routes::room_keys::delete_room_keys_all),
        )
        .route(
            "/room_keys/keys/{room_id}",
            get(routes::room_keys::get_room_keys_room)
                .put(routes::room_keys::put_room_keys_room)
                .delete(routes::room_keys::delete_room_keys_room),
        )
        .route(
            "/room_keys/keys/{room_id}/{session_id}",
            get(routes::room_keys::get_room_keys_session)
                .put(routes::room_keys::put_room_keys_session)
                .delete(routes::room_keys::delete_room_keys_session),
        )
        .route("/voip/turnServer", get(routes::voip::get_voip_turn_server))
        .route("/pushers", get(routes::pushers::get_pushers))
        .route("/pushers/set", post(routes::pushers::post_pushers_set))
        .route(
            "/sendToDevice/{event_type}/{txn_id}",
            put(routes::to_device::put_send_to_device),
        )
        .route(
            "/rooms/{room_id}/read_markers",
            post(routes::ephemeral::post_read_markers),
        )
        .route(
            "/rooms/{room_id}/receipt/{receipt_type}/{event_id}",
            post(routes::ephemeral::post_receipt),
        )
        .route(
            "/rooms/{room_id}/typing/{user_id}",
            put(routes::ephemeral::put_typing),
        )
        // ── Account data + room tags ──────────────────────────────────────────
        .route(
            "/user/{user_id}/account_data/{event_type}",
            put(routes::account_data::put_account_data_global)
                .get(routes::account_data::get_account_data_global),
        )
        .route(
            "/user/{user_id}/rooms/{room_id}/account_data/{event_type}",
            put(routes::account_data::put_account_data_room)
                .get(routes::account_data::get_account_data_room),
        )
        .route(
            "/user/{user_id}/rooms/{room_id}/tags",
            get(routes::account_data::get_room_tags),
        )
        .route(
            "/user/{user_id}/rooms/{room_id}/tags/{tag}",
            put(routes::account_data::put_room_tag).delete(routes::account_data::delete_room_tag),
        )
}
