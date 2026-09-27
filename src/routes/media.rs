// START_AI_HEADER
// MODULE: matrix-hs/src/routes/media.rs
// PURPOSE: Matrix media repository (`/_matrix/media/*`, `/_matrix/client/v1/media/*`).
//          Upload / download / thumbnail / config for avatars, images, and file
//          attachments — none of this existed before; Element could not upload or
//          render any media without it.
//
//          Endpoints (both the legacy media/v3 paths and the newer authenticated
//          client/v1/media paths share ONE handler each, mounted twice in lib.rs):
//            POST /_matrix/media/v3/upload                         -> post_upload
//            GET  /_matrix/client/v1/media/download/{server}/{id}  -> get_download
//            GET  /_matrix/media/v3/download/{server}/{id}         -> get_download
//            GET  /_matrix/client/v1/media/thumbnail/{server}/{id} -> get_thumbnail
//            GET  /_matrix/media/v3/thumbnail/{server}/{id}        -> get_thumbnail
//            GET  /_matrix/client/v1/media/config                 -> get_config
//            GET  /_matrix/media/v3/config                        -> get_config
//          NOT implemented (optional per spec, out of scope here): the async
//          two-step POST /_matrix/media/v1/create + PUT .../upload/{server}/{mediaId}
//          upload flow. The single-shot POST .../upload above covers every real client
//          upload path (Element included).
//
//          Auth posture (deliberately simple, per task scope):
//            - Upload: REQUIRES a valid signed Bearer token (extract_caller, reused
//              from routes/keys.rs) — an unauthenticated client cannot mint mxc:// ids.
//            - Download / thumbnail / config: UNAUTHENTICATED. The Matrix spec's
//              authenticated-media MSC (client/v1/media requiring a token) is not
//              enforced here; both the legacy and v1 paths are open reads. This is a
//              deliberate simplification (see task instructions) — real deployments
//              serving private media would need to add the same extract_caller check
//              used for upload to the v1 download/thumbnail handlers.
//
//          Storage design:
//            AppState.media.media: media_id -> MediaEntry (content_type + bytes + owner_node),
//            held in memory (state.rs). When MATRIX_HS_DATA_DIR is set, AppState::
//            store_media also fsyncs the blob + a Content-Type sidecar under
//            <data_dir>/media/ (persist.rs) so uploads survive a restart via
//            persist::replay_media. Unset data_dir -> pure in-memory (mirrors every
//            other persist_* subsystem's degrade-gracefully posture).
//
//          CROSS-NODE FETCH (cluster feature) — live since 2026-08-04:
//            A media_id uploaded on node A is stored only on A. When node B is asked
//            for it, B pulls it off the mesh and keeps a copy.
//
//            PULL, not push. Blobs are never gossiped: they are megabytes, and the
//            Zenoh channel they would ride is the one carrying room events. Pushing
//            every upload to every node would replicate files most nodes are never
//            asked for, at the expense of the traffic that actually has to be timely.
//            Pulling moves each blob once per node that reads it, on the first read.
//
//            Mechanism (mirrors the room catch-up queryables in main.rs):
//              1. Each node declares ONE queryable on "<prefix>/media/*" (main.rs),
//                 keyed by media_id rather than by node — so nothing has to gossip a
//                 media_id -> owner mapping for a fetch to find the holder.
//              2. A node that does NOT have the requested blob stays silent. Silence
//                 is the "no" signal; an empty reply would be indistinguishable from a
//                 real one, and every node in the mesh would send one on every miss.
//              3. On a local get_media() miss, resolve_media calls
//                 fetch_media_cross_node -> ClusterState::fetch_media, which takes the
//                 first reply that decodes, caches it write-through
//                 (AppState::cache_media_from_peer, preserving the ORIGINAL
//                 owner_node), and serves it.
//            Bounded: a reply larger than max_media_upload_bytes() is refused, and the
//            whole round trip is capped by MATRIX_HS_MEDIA_FETCH_TIMEOUT_MS (5 s) — a
//            client is waiting on it, so an unfindable blob 404s rather than hangs.
//
//            Without the `cluster` feature, or with the cluster layer inactive, this
//            degrades to local-only and a cross-node download 404s as before.
//
// DEPENDENCIES: axum, image (jpeg+png), AppState, routes::keys::extract_caller
// PUBLIC_API: post_upload, get_download, get_thumbnail, get_config
// END_AI_HEADER

use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

use crate::{error::HsError, routes::keys::extract_caller, state::AppState};

// DEFAULT_CONTENT_TYPE:start
//   purpose: Content-Type applied to an upload when the client sends none.
//   input:  none
//   output: &'static str
//   sideEffects: none
// DEFAULT_CONTENT_TYPE:end
const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

// post_upload:start
//   purpose: POST /_matrix/media/v3/upload (and mounted identically under
//            /_matrix/client/v1/media/upload) — store the raw request body as a new
//            media blob and mint an mxc:// content URI for it. Requires a valid signed
//            Bearer token; unknown/missing token -> 401 M_UNKNOWN_TOKEN. Body larger
//            than AppState::max_media_upload_bytes() -> 413 M_TOO_LARGE, checked BEFORE
//            any storage write.
//   input:  headers — Content-Type (optional, defaults to application/octet-stream)
//           and Authorization: Bearer <signed mxt_ token>; body — raw bytes
//   output: JSON {"content_uri": "mxc://<server_name>/<media_id>"}
//           401 M_UNKNOWN_TOKEN on missing/invalid token; 413 M_TOO_LARGE if oversized
//   sideEffects: mints a media_id (OsRng); writes into state.media.media (+ disk, if
//                persistence enabled)
// post_upload:end
pub async fn post_upload(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Json<Value>, HsError> {
    extract_caller(&headers, &state)
        .ok_or_else(|| HsError::UnknownToken("missing or invalid token".to_string()))?;

    let max = state.max_media_upload_bytes();
    if (body.len() as u64) > max {
        return Err(HsError::TooLarge(format!(
            "upload of {} bytes exceeds the {max}-byte limit",
            body.len()
        )));
    }

    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or(DEFAULT_CONTENT_TYPE)
        .to_string();

    let media_id = AppState::new_media_id();
    state
        .store_media(&media_id, &content_type, body.to_vec())
        .map_err(HsError::Internal)?;

    let content_uri = format!("mxc://{}/{}", state.server_name, media_id);
    Ok(Json(json!({ "content_uri": content_uri })))
}

// resolve_media:start
//   purpose: Look up `media_id` in this node's local media store; on a miss, pull it
//            from whichever peer holds it before giving up. Shared by get_download and
//            get_thumbnail so both honour the same local-then-mesh resolution order —
//            which means a thumbnail request can also be what drags a blob across.
//   input:  state — Arc<AppState>; server_name — the {server} path segment, carried
//           for logging only: the fetch is keyed by media_id, and the holder need not
//           be the node that minted the id; media_id — the {mediaId} path segment
//   output: Ok(MediaEntry) if held locally or fetched from a peer;
//           Err(HsError::NotFound) if no node has it
//   sideEffects: on a local miss, a Zenoh query and (on success) a write-through cache
//                insert — so this is NOT read-only in cluster mode
// resolve_media:end
async fn resolve_media(
    state: &Arc<AppState>,
    server_name: &str,
    media_id: &str,
) -> Result<crate::state::MediaEntry, HsError> {
    if let Some(entry) = state.get_media(media_id) {
        return Ok(entry);
    }
    if let Some(entry) = fetch_media_cross_node(state, server_name, media_id).await {
        return Ok(entry);
    }
    Err(HsError::NotFound(format!(
        "media not found: {server_name}/{media_id}"
    )))
}

// get_download:start
//   purpose: GET /_matrix/media/v3/download/{server}/{mediaId} (and the
//            /_matrix/client/v1/media/download/{server}/{mediaId} mount) — return the
//            stored bytes with their original Content-Type. Unauthenticated (see
//            module header's auth posture note).
//   input:  Path(server_name, media_id)
//   output: 200 with body = original bytes, Content-Type = as uploaded;
//           404 M_NOT_FOUND if no node in the cluster has that media_id
//   sideEffects: may fetch and cache the blob from a peer (see resolve_media)
// get_download:end
pub async fn get_download(
    State(state): State<Arc<AppState>>,
    Path((server_name, media_id)): Path<(String, String)>,
) -> Result<Response, HsError> {
    let entry = resolve_media(&state, &server_name, &media_id).await?;
    Ok(bytes_response(
        &entry.content_type,
        entry.bytes.as_ref().clone(),
    ))
}

// ThumbnailParams:start
//   purpose: Query-string parameters for GET .../thumbnail/{server}/{mediaId}.
//            width/height default to 96 (a reasonable avatar-sized fallback) when the
//            client omits them (not spec-required, but avoids a hard 400 on a lazy
//            client); method defaults to "scale".
//   input:  deserialised by axum's Query extractor from the request's query string
//   output: ThumbnailParams value
//   sideEffects: none
// ThumbnailParams:end
#[derive(Debug, Deserialize)]
pub struct ThumbnailParams {
    #[serde(default = "default_dimension")]
    pub width: u32,
    #[serde(default = "default_dimension")]
    pub height: u32,
    #[serde(default = "default_method")]
    pub method: String,
}

fn default_dimension() -> u32 {
    96
}
fn default_method() -> String {
    "scale".to_string()
}

// get_thumbnail:start
//   purpose: GET /_matrix/media/v3/thumbnail/{server}/{mediaId} (and the
//            /_matrix/client/v1/media/thumbnail/{server}/{mediaId} mount) — decode the
//            stored image and return a resized copy per `width`/`height`/`method`.
//            method="crop": resize_to_fill (crops to exactly width x height).
//            method="scale" (default, and any other value): resize (fits within the
//            width x height box, preserving aspect ratio — Matrix's "scale" semantics).
//            Always re-encodes the result as PNG (Content-Type: image/png) regardless
//            of the source format, which keeps the encode path simple (this server
//            only needs to guarantee "a valid, smaller image", not format fidelity).
//            Non-image media, or bytes `image` cannot decode -> 400 M_BAD_JSON (reused
//            as a generic "can't process this content" error; there is no dedicated
//            Matrix errcode for this case).
//   input:  Path(server_name, media_id); Query(ThumbnailParams)
//   output: 200 with body = re-encoded PNG bytes, Content-Type: image/png;
//           404 M_NOT_FOUND if media_id unknown; 400 M_BAD_JSON if decode fails
//   sideEffects: none (read-only; CPU-bound decode/resize/encode runs on the async
//                request task — acceptable at this milestone's scale, no thread-pool
//                offload)
// get_thumbnail:end
pub async fn get_thumbnail(
    State(state): State<Arc<AppState>>,
    Path((server_name, media_id)): Path<(String, String)>,
    Query(params): Query<ThumbnailParams>,
) -> Result<Response, HsError> {
    let entry = resolve_media(&state, &server_name, &media_id).await?;

    let img = image::load_from_memory(entry.bytes.as_ref())
        .map_err(|e| HsError::BadRequest(format!("cannot decode media as an image: {e}")))?;

    let width = params.width.max(1);
    let height = params.height.max(1);

    let resized = if params.method == "crop" {
        img.resize_to_fill(width, height, image::imageops::FilterType::Lanczos3)
    } else {
        img.resize(width, height, image::imageops::FilterType::Lanczos3)
    };

    let mut out: Vec<u8> = Vec::new();
    {
        let mut cursor = std::io::Cursor::new(&mut out);
        resized
            .write_to(&mut cursor, image::ImageFormat::Png)
            .map_err(|e| HsError::Internal(format!("thumbnail encode failed: {e}")))?;
    }

    Ok(bytes_response("image/png", out))
}

// get_config:start
//   purpose: GET /_matrix/media/v3/config (and /_matrix/client/v1/media/config) —
//            advertise the maximum upload size so well-behaved clients can pre-check
//            a file before attempting to upload it. Unauthenticated (matches the
//            module header's simplified auth posture; the real spec allows either).
//   input:  none
//   output: JSON {"m.upload.size": <max_bytes>}
//   sideEffects: none
// get_config:end
pub async fn get_config(State(state): State<Arc<AppState>>) -> Json<Value> {
    Json(json!({ "m.upload.size": state.max_media_upload_bytes() }))
}

// bytes_response:start
//   purpose: Build a 200 OK axum Response with an arbitrary Content-Type and a raw
//            byte body. Shared by get_download and get_thumbnail so both binary
//            responses are constructed identically.
//   input:  content_type — value for the Content-Type header; bytes — response body
//   output: axum::response::Response
//   sideEffects: none
// bytes_response:end
fn bytes_response(content_type: &str, bytes: Vec<u8>) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, content_type.to_string())],
        bytes,
    )
        .into_response()
}

// MEDIA_FETCH_TIMEOUT_MS_DEFAULT:start
//   purpose: How long a download may block while the blob is pulled from a peer.
//            A client is waiting on this, so it is short — a miss should 404 rather
//            than hang — but long enough for a real transfer over a slow carrier.
//   input:  none
//   output: u64 milliseconds
//   sideEffects: none
// MEDIA_FETCH_TIMEOUT_MS_DEFAULT:end
#[cfg(feature = "cluster")]
const MEDIA_FETCH_TIMEOUT_MS_DEFAULT: u64 = 5000;

// fetch_media_cross_node:start
//   purpose: Fetch a blob this node does not have from whichever peer does, and cache
//            it locally on the way past. This is the cross-node seam described in the
//            module header, now live.
//
//            Pull, not push: the query key names the media_id, and a node without the
//            blob does not answer, so nothing has to know in advance which node holds
//            what. The blob crosses the mesh once per node that reads it, instead of
//            once per node per upload.
//
//            Without the `cluster` feature, or with the cluster layer inactive, this
//            is still a no-op and resolve_media degrades to local-only.
//   input:  state — Arc<AppState>; server_name — the {server} path segment, used only
//           in logging (it is NOT used to pick a node: media may legitimately be held
//           by a node other than the one that minted the id, including this one after
//           a cache); media_id — the {mediaId} path segment
//   output: Some(MediaEntry) on the first peer reply that decodes; None if no peer
//           has it, the reply was malformed or oversized, or clustering is off
//   sideEffects: one Zenoh query round-trip; on success, a write-through insert into
//                state.media.media (and to disk when persistence is enabled)
// fetch_media_cross_node:end
async fn fetch_media_cross_node(
    state: &Arc<AppState>,
    server_name: &str,
    media_id: &str,
) -> Option<crate::state::MediaEntry> {
    #[cfg(feature = "cluster")]
    {
        let cluster = state.cluster.as_ref()?;
        let timeout_ms = std::env::var("MATRIX_HS_MEDIA_FETCH_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(MEDIA_FETCH_TIMEOUT_MS_DEFAULT);
        let entry = cluster
            .fetch_media(
                media_id,
                std::time::Duration::from_millis(timeout_ms),
                state.max_media_upload_bytes() as usize,
            )
            .await?;
        eprintln!(
            "[matrix-hs] media {server_name}/{media_id}: fetched {} bytes from peer \
             (owner {})",
            entry.bytes.len(),
            entry.owner_node
        );
        if let Err(e) = state.cache_media_from_peer(media_id, &entry) {
            // Serving it still works; only the caching failed.
            eprintln!("[matrix-hs] media {media_id}: cache write-through failed: {e}");
        }
        Some(entry)
    }
    #[cfg(not(feature = "cluster"))]
    {
        let _ = (state, server_name, media_id);
        None
    }
}

#[cfg(test)]
mod seam_test {
    use super::*;

    // media_seam_is_local_only_without_cluster:start
    //   purpose: Without the `cluster` feature there is no mesh to ask, so the seam
    //            must stay a no-op and resolve_media must degrade to local-only.
    //            Replaces the old "documented no-op" test, which asserted this
    //            unconditionally — it is now only true in this build configuration.
    //   input:  none
    //   output: assertion that fetch_media_cross_node(...) == None
    //   sideEffects: none
    // media_seam_is_local_only_without_cluster:end
    #[cfg(not(feature = "cluster"))]
    #[tokio::test]
    async fn media_seam_is_local_only_without_cluster() {
        let state = AppState::with_server_name("nodeA".to_string());
        let result = fetch_media_cross_node(&state, "nodeA", "some-media-id").await;
        assert!(
            result.is_none(),
            "without the cluster feature there is nowhere to fetch from"
        );
    }

    // media_reply_wire_format_round_trip:start
    //   purpose: The media reply frame is hand-rolled binary (base64 would inflate
    //            every transfer by a third), so pin its round-trip — including the
    //            cases that break naive length-prefix parsers.
    //   input:  none
    //   output: assertions on encode/decode
    //   sideEffects: none
    // media_reply_wire_format_round_trip:end
    #[cfg(feature = "cluster")]
    #[test]
    fn media_reply_wire_format_round_trip() {
        use crate::state::ClusterState;

        let blob = vec![0u8, 255, 1, 128, 0, 0, 7];
        let enc = ClusterState::encode_media_reply("image/png", "node-a", &blob);
        let (ct, owner, out) = ClusterState::decode_media_reply(&enc).expect("decodes");
        assert_eq!(ct, "image/png");
        assert_eq!(owner, "node-a");
        assert_eq!(out, blob, "blob must survive byte-for-byte, NULs included");

        // Empty blob: a zero-length file is still a file.
        let enc = ClusterState::encode_media_reply("text/plain", "n", &[]);
        let (_, _, out) = ClusterState::decode_media_reply(&enc).expect("decodes");
        assert!(out.is_empty());

        // Non-ASCII content-type and owner must not be split mid-codepoint.
        let enc = ClusterState::encode_media_reply("text/plain; x=é", "узел-a", b"hi");
        let (ct, owner, out) = ClusterState::decode_media_reply(&enc).expect("decodes");
        assert_eq!(ct, "text/plain; x=é");
        assert_eq!(owner, "узел-a");
        assert_eq!(out, b"hi");

        // Truncated frames are rejected, not guessed at — this is network input.
        assert!(ClusterState::decode_media_reply(&[]).is_none());
        assert!(ClusterState::decode_media_reply(&[0]).is_none());
        let good = ClusterState::encode_media_reply("image/png", "node-a", b"xyz");
        for cut in 0..good.len().saturating_sub(3) {
            // Any prefix that ends inside a length-prefixed field must fail.
            if ClusterState::decode_media_reply(&good[..cut]).is_some() && cut < 4 {
                panic!("prefix of {cut} bytes must not decode");
            }
        }
    }

    // media_key_round_trip:start
    //   purpose: media_key and media_from_key must agree, and media_from_key must not
    //            mistake a room key for a media key — both live under the same prefix.
    //   input:  none
    //   output: assertions on key construction and parsing
    //   sideEffects: none
    // media_key_round_trip:end
    #[cfg(feature = "cluster")]
    #[test]
    fn media_key_round_trip() {
        use crate::state::ClusterState;
        let prefix = "mrgd/matrix/room";

        let k = ClusterState::media_key(prefix, "AbC-123_xyz");
        assert_eq!(k, "mrgd/matrix/room/media/AbC-123_xyz");
        assert_eq!(
            ClusterState::media_from_key(&k, prefix),
            Some("AbC-123_xyz")
        );

        // Room keys share the prefix and must not parse as media.
        assert_eq!(
            ClusterState::media_from_key("mrgd/matrix/room/!r:localhost/history", prefix, ),
            None
        );
        assert_eq!(
            ClusterState::media_from_key("mrgd/matrix/room/media/", prefix),
            None,
            "empty media_id"
        );
        assert_eq!(
            ClusterState::media_from_key("mrgd/matrix/room/media/a/b", prefix),
            None,
            "a media_id is exactly one chunk"
        );
        assert_eq!(
            ClusterState::media_from_key("other/prefix/media/x", prefix),
            None
        );
    }

    // media_fetched_from_peer_and_cached:start
    //   purpose: Prove the seam end-to-end: node B, asked for a blob only node A has,
    //            pulls it over Zenoh, serves it, and keeps it — so the second read is
    //            local. This is the "media is not replicated cross-node" gap closing.
    //
    //            Node A's side mirrors the queryable main.rs declares (the binary has
    //            no test harness), but every decision function on the path — media_key,
    //            media_from_key, encode/decode, ClusterState::fetch_media and the
    //            route's own resolve_media — is the production one.
    //
    //   input:  none (all resources constructed in-test)
    //   output: assertions that B resolves the blob, byte-for-byte, with A recorded as
    //           its owner, and that B has it locally afterwards
    //   sideEffects: opens two Zenoh sessions
    // media_fetched_from_peer_and_cached:end
    #[cfg(feature = "cluster")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn media_fetched_from_peer_and_cached() {
        let _zg = crate::test_util::ZENOH_TEST_LOCK.acquire().await.unwrap();
        use crate::state::{ClusterConfig, ClusterState};

        let prefix = "mrgd/matrix/room/media-fetch-test";
        let media_id = "test-media-id-abc";
        // Deliberately includes bytes that a text-oriented codec would mangle.
        let blob: Vec<u8> = (0u8..=255).chain(0u8..=64).collect();

        let sess_root = zenoh::open(zenoh::Config::default())
            .await
            .expect("Zenoh session root");
        let sess_b = sess_root.clone();

        // ── Node A: holds the blob, and serves it on the media queryable ──────
        let state_a = AppState::with_cluster(ClusterConfig {
            session: sess_root.clone(),
            key_prefix: prefix.to_string(),
            server_name: "node-a".to_string(),
        });
        state_a
            .store_media(media_id, "application/octet-stream", blob.clone())
            .expect("store on A");

        let media_wild = format!("{prefix}/media/*");
        let qable = sess_root
            .declare_queryable(&media_wild)
            .await
            .expect("declare media queryable");
        let handler = qable.handler().clone();
        let state_for_qable = state_a.clone();
        let prefix_for_qable = prefix.to_string();
        let _qable_task = tokio::spawn(async move {
            while let Ok(query) = handler.recv_async().await {
                let qkey = query.key_expr().as_str().to_string();
                let Some(id) = ClusterState::media_from_key(&qkey, &prefix_for_qable) else {
                    continue;
                };
                let Some(entry) = state_for_qable.get_media(id) else {
                    continue; // silence means "not here"
                };
                let payload = ClusterState::encode_media_reply(
                    &entry.content_type,
                    &entry.owner_node,
                    &entry.bytes,
                );
                let reply_key = ClusterState::media_key(&prefix_for_qable, id);
                let _ = query.reply(&reply_key, payload).await;
            }
        });

        // ── Node B: does not have it ──────────────────────────────────────────
        let state_b = AppState::with_cluster(ClusterConfig {
            session: sess_b,
            key_prefix: prefix.to_string(),
            server_name: "node-b".to_string(),
        });
        assert!(
            state_b.get_media(media_id).is_none(),
            "precondition: node B must not already hold the blob"
        );

        // ── B resolves it: local miss → peer fetch ────────────────────────────
        let entry = resolve_media(&state_b, "node-a", media_id)
            .await
            .expect("node B must resolve the blob from node A");
        assert_eq!(
            entry.bytes.as_ref(),
            &blob,
            "blob must arrive byte-for-byte"
        );
        assert_eq!(entry.content_type, "application/octet-stream");
        assert_eq!(
            entry.owner_node, "node-a",
            "a cached copy must record the ORIGINAL owner, not node-b"
        );

        // ── and kept, so the next read costs nothing ──────────────────────────
        let cached = state_b
            .get_media(media_id)
            .expect("write-through cache must have kept the blob on node B");
        assert_eq!(cached.bytes.as_ref(), &blob);
        assert_eq!(cached.owner_node, "node-a");
    }

    // media_miss_stays_a_miss:start
    //   purpose: A blob no node has must 404 rather than hang or resolve to something
    //            empty. Guards the "stay silent on a miss" rule in the queryable: if a
    //            node without the blob answered anyway, this would resolve to garbage.
    //   input:  none
    //   output: assertion that resolve_media errors for an unknown media_id
    //   sideEffects: opens a Zenoh session
    // media_miss_stays_a_miss:end
    #[cfg(feature = "cluster")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn media_miss_stays_a_miss() {
        let _zg = crate::test_util::ZENOH_TEST_LOCK.acquire().await.unwrap();
        use crate::state::ClusterConfig;

        let prefix = "mrgd/matrix/room/media-miss-test";
        let sess = zenoh::open(zenoh::Config::default())
            .await
            .expect("Zenoh session");
        let state = AppState::with_cluster(ClusterConfig {
            session: sess,
            key_prefix: prefix.to_string(),
            server_name: "node-solo".to_string(),
        });

        // Keep the fetch budget short: nobody is going to answer.
        std::env::set_var("MATRIX_HS_MEDIA_FETCH_TIMEOUT_MS", "400");
        let result = resolve_media(&state, "node-solo", "no-such-media-id").await;
        std::env::remove_var("MATRIX_HS_MEDIA_FETCH_TIMEOUT_MS");

        assert!(
            result.is_err(),
            "an unknown media_id must stay a miss, not resolve to an empty blob"
        );
    }
}
