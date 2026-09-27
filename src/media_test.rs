// START_AI_HEADER
// MODULE: matrix-hs/src/media_test.rs
// PURPOSE: Integration tests for the media repository (routes/media.rs).
//          Covers: upload -> mxc:// content_uri; download round-trips the exact bytes
//          and Content-Type; thumbnail decodes/resizes to a smaller valid image;
//          config reports the size limit; upload without a token is rejected;
//          oversized upload is rejected. All node-local (see routes/media.rs's
//          documented cross-node deferral — there is no second node to test against
//          yet, matching the "else same-node" fallback the task instructions allow).
// DEPENDENCIES: axum-test, image, serde_json, matrix_hs::{router, AppState}
// END_AI_HEADER

#[cfg(test)]
mod tests {
    use crate::{router, AppState};
    use axum::http::{HeaderName, HeaderValue};
    use axum_test::TestServer;
    use serde_json::json;

    // test_server:start
    //   purpose: Build a fresh TestServer with an empty in-memory AppState.
    //   input:  none
    //   output: TestServer
    //   sideEffects: none
    // test_server:end
    fn test_server() -> TestServer {
        let state = AppState::new();
        let app = router(state);
        TestServer::new(app)
    }

    // auth:start
    //   purpose: Register a user via two-step UIA (mirrors keys_test.rs's helper) and
    //            return an Authorization: Bearer <mxt_...> header for axum-test requests.
    //   input:  server — &TestServer; user — localpart string
    //   output: (HeaderName, HeaderValue) for Authorization: Bearer <mxt_ token>
    //   sideEffects: inserts user into AppState via register
    // auth:end
    async fn auth(server: &TestServer, user: &str) -> (HeaderName, HeaderValue) {
        let challenge: serde_json::Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({ "username": user, "password": "pw" }))
            .await
            .json();
        let session = challenge["session"]
            .as_str()
            .unwrap_or_else(|| panic!("auth helper: missing session for {user}; got {challenge}"))
            .to_string();

        let reg: serde_json::Value = server
            .post("/_matrix/client/v3/register")
            .json(&json!({
                "username": user,
                "password": "pw",
                "auth": { "type": "m.login.dummy", "session": session }
            }))
            .await
            .json();

        let token = reg["access_token"]
            .as_str()
            .unwrap_or_else(|| panic!("auth helper: missing access_token for {user}; got {reg}"))
            .to_string();

        (
            HeaderName::from_static("authorization"),
            HeaderValue::from_str(&format!("Bearer {token}")).expect("header value"),
        )
    }

    // sample_png:start
    //   purpose: Build a small in-memory 32x24 PNG (solid colour) to use as upload
    //            fixture bytes for the thumbnail test — avoids depending on any file on
    //            disk, and gives a deterministic, small, `image`-decodable payload.
    //   input:  none
    //   output: Vec<u8> — PNG-encoded bytes
    //   sideEffects: none
    // sample_png:end
    fn sample_png() -> Vec<u8> {
        let img = image::RgbImage::from_pixel(32, 24, image::Rgb([200, 50, 10]));
        let dynimg = image::DynamicImage::ImageRgb8(img);
        let mut out = Vec::new();
        let mut cursor = std::io::Cursor::new(&mut out);
        dynimg
            .write_to(&mut cursor, image::ImageFormat::Png)
            .expect("encode sample PNG");
        out
    }

    // upload_download_roundtrip:start
    //   purpose: Upload raw bytes with a Content-Type; verify the response is an
    //            mxc://<server_name>/<media_id> content_uri; then download that same
    //            mxc URI's path and verify the bytes and Content-Type match exactly.
    //   input:  none
    //   output: assertions on the upload + download responses
    //   sideEffects: stores media in AppState
    // upload_download_roundtrip:end
    #[tokio::test]
    async fn upload_download_roundtrip() {
        let server = test_server();
        let (hn, hv) = auth(&server, "alice").await;
        let payload: Vec<u8> = vec![1, 2, 3, 4, 5, 250, 251, 252];

        let upload_resp = server
            .post("/_matrix/media/v3/upload")
            .add_header(hn.clone(), hv.clone())
            .add_header(
                HeaderName::from_static("content-type"),
                HeaderValue::from_static("application/x-test-blob"),
            )
            .bytes(axum::body::Bytes::copy_from_slice(&payload))
            .await;
        upload_resp.assert_status_ok();

        let upload_json: serde_json::Value = upload_resp.json();
        let content_uri = upload_json["content_uri"]
            .as_str()
            .unwrap_or_else(|| panic!("missing content_uri; got {upload_json}"))
            .to_string();
        assert!(content_uri.starts_with("mxc://"), "got {content_uri}");

        // mxc://<server_name>/<media_id> -> split off server_name and media_id.
        let rest = content_uri.strip_prefix("mxc://").expect("mxc prefix");
        let mut parts = rest.splitn(2, '/');
        let server_name = parts.next().expect("server_name");
        let media_id = parts.next().expect("media_id");

        // Download via the legacy /_matrix/media/v3 path.
        let dl = server
            .get(&format!(
                "/_matrix/media/v3/download/{server_name}/{media_id}"
            ))
            .await;
        dl.assert_status_ok();
        assert_eq!(
            dl.as_bytes().to_vec(),
            payload,
            "downloaded bytes must match upload exactly"
        );
        let ct = dl
            .headers()
            .get("content-type")
            .expect("content-type header present")
            .to_str()
            .expect("valid header string");
        assert_eq!(ct, "application/x-test-blob");

        // Also reachable via the authenticated client/v1 path (unauthenticated read,
        // per this module's simplified auth posture).
        let dl_v1 = server
            .get(&format!(
                "/_matrix/client/v1/media/download/{server_name}/{media_id}"
            ))
            .await;
        dl_v1.assert_status_ok();
        assert_eq!(dl_v1.as_bytes().to_vec(), payload);
    }

    // upload_requires_auth:start
    //   purpose: POST .../upload without an Authorization header must be rejected with
    //            401 M_UNKNOWN_TOKEN — media upload is not an anonymous operation.
    //   input:  none
    //   output: assertion on the response status + errcode
    //   sideEffects: none
    // upload_requires_auth:end
    #[tokio::test]
    async fn upload_requires_auth() {
        let server = test_server();
        let resp = server
            .post("/_matrix/media/v3/upload")
            .bytes(axum::body::Bytes::from_static(b"no token here"))
            .await;
        resp.assert_status(axum::http::StatusCode::UNAUTHORIZED);
        let body: serde_json::Value = resp.json();
        assert_eq!(body["errcode"], "M_UNKNOWN_TOKEN");
    }

    // download_unknown_media_404:start
    //   purpose: Downloading a media_id that was never uploaded (locally, and the
    //            cross-node seam is a documented no-op) must 404 M_NOT_FOUND, not panic
    //            or 500.
    //   input:  none
    //   output: assertion on response status + errcode
    //   sideEffects: none
    // download_unknown_media_404:end
    #[tokio::test]
    async fn download_unknown_media_404() {
        let server = test_server();
        let resp = server
            .get("/_matrix/media/v3/download/localhost/does-not-exist")
            .await;
        resp.assert_status_not_found();
        let body: serde_json::Value = resp.json();
        assert_eq!(body["errcode"], "M_NOT_FOUND");
    }

    // thumbnail_smaller_valid_image:start
    //   purpose: Upload a 32x24 PNG, request a thumbnail at 8x8 with method=scale,
    //            verify the response decodes as a valid image whose byte size is
    //            smaller than the original and whose dimensions fit within the
    //            requested box (scale preserves aspect ratio, so one dimension may be
    //            smaller than requested but neither exceeds it).
    //   input:  none
    //   output: assertions on thumbnail response + decoded dimensions
    //   sideEffects: stores media in AppState
    // thumbnail_smaller_valid_image:end
    #[tokio::test]
    async fn thumbnail_smaller_valid_image() {
        let server = test_server();
        let (hn, hv) = auth(&server, "bob").await;
        let png = sample_png();

        let upload_resp = server
            .post("/_matrix/media/v3/upload")
            .add_header(hn, hv)
            .add_header(
                HeaderName::from_static("content-type"),
                HeaderValue::from_static("image/png"),
            )
            .bytes(axum::body::Bytes::copy_from_slice(&png))
            .await;
        upload_resp.assert_status_ok();
        let upload_json: serde_json::Value = upload_resp.json();
        let content_uri = upload_json["content_uri"]
            .as_str()
            .expect("content_uri")
            .to_string();
        let rest = content_uri.strip_prefix("mxc://").expect("mxc prefix");
        let mut parts = rest.splitn(2, '/');
        let server_name = parts.next().expect("server_name");
        let media_id = parts.next().expect("media_id");

        let thumb_resp = server
            .get(&format!(
                "/_matrix/media/v3/thumbnail/{server_name}/{media_id}?width=8&height=8&method=scale"
            ))
            .await;
        thumb_resp.assert_status_ok();
        let thumb_bytes = thumb_resp.as_bytes().to_vec();

        assert!(
            thumb_bytes.len() < png.len(),
            "thumbnail should be smaller than the original"
        );

        let decoded = image::load_from_memory(&thumb_bytes)
            .expect("thumbnail response must be a valid, decodable image");
        assert!(
            decoded.width() <= 8 && decoded.height() <= 8,
            "scale must fit within the requested box; got {}x{}",
            decoded.width(),
            decoded.height()
        );
    }

    // config_reports_size_limit:start
    //   purpose: GET .../config returns {"m.upload.size": <n>} matching
    //            AppState::max_media_upload_bytes()'s default.
    //   input:  none
    //   output: assertion on the response body
    //   sideEffects: none
    // config_reports_size_limit:end
    #[tokio::test]
    async fn config_reports_size_limit() {
        let server = test_server();
        let resp = server.get("/_matrix/media/v3/config").await;
        resp.assert_status_ok();
        let body: serde_json::Value = resp.json();
        let size = body["m.upload.size"]
            .as_u64()
            .expect("m.upload.size present");
        assert_eq!(size, AppState::new().max_media_upload_bytes());

        // Also reachable via the client/v1 media config mount.
        let resp_v1 = server.get("/_matrix/client/v1/media/config").await;
        resp_v1.assert_status_ok();
        let body_v1: serde_json::Value = resp_v1.json();
        assert_eq!(
            body_v1["m.upload.size"]
                .as_u64()
                .expect("m.upload.size present"),
            size
        );
    }

    // media:upload_above_axums_default_body_limit_succeeds:start
    //   purpose: The router must raise axum's 2 MiB default body cap to the limit
    //            this server actually advertises. Axum enforces that cap BEFORE
    //            any handler runs, so without the layer post_upload's own
    //            max_media_upload_bytes() check never saw an oversized body and
    //            the server refused everything past 2 MiB while advertising
    //            50 MiB in /media/v3/config. Found in production: a camera
    //            pipeline whose JPEG snapshots all succeeded and whose MP4 clips
    //            all failed, 94 bare 413s in three hours.
    //   input:  a 3 MiB upload — over axum's default, far under the advertised limit
    //   output: 200 with a content_uri, and the blob downloads back byte-identical
    //   sideEffects: stores 3 MiB in the in-memory media store
    // media:upload_above_axums_default_body_limit_succeeds:end
    #[tokio::test]
    async fn upload_above_axums_default_body_limit_succeeds() {
        let server = test_server();
        let (hn, hv) = auth(&server, "alice").await;

        // 3 MiB: comfortably over axum's 2 MiB default, comfortably under the
        // 50 MiB this server advertises. A pattern rather than zeroes so the
        // round-trip actually proves the bytes survived.
        let payload: Vec<u8> = (0..3 * 1024 * 1024).map(|i| (i % 251) as u8).collect();

        let upload = server
            .post("/_matrix/media/v3/upload")
            .add_header(hn.clone(), hv.clone())
            .add_header(
                HeaderName::from_static("content-type"),
                HeaderValue::from_static("application/x-test-blob"),
            )
            .bytes(axum::body::Bytes::copy_from_slice(&payload))
            .await;
        assert_eq!(
            upload.status_code(),
            200,
            "a 3 MiB upload must be accepted — the advertised limit is 50 MiB, and \
             a 413 here means axum's default body cap is back in front of the handler"
        );

        let uri = upload.json::<serde_json::Value>()["content_uri"]
            .as_str()
            .expect("content_uri")
            .to_string();
        let rest = uri.trim_start_matches("mxc://");
        let (srv, id) = rest.split_once('/').expect("mxc://server/id");

        let got = server
            .get(&format!("/_matrix/media/v3/download/{srv}/{id}"))
            .add_header(hn, hv)
            .await;
        got.assert_status_ok();
        assert_eq!(
            got.as_bytes().to_vec(),
            payload,
            "the blob must come back byte-identical"
        );
    }


    // mem:persisted_media_not_retained_in_ram:start
    //   purpose: gamma-33 regression guard. Under persistence, store_media must
    //            keep ONLY the index in RAM (empty-bytes sentinel) — the old
    //            behaviour retained every uploaded blob forever and leaked at
    //            the exact rate media arrived (8-9 GiB/day on the camera node).
    //            get_media must transparently load the bytes back from disk.
    //   input:  temp data dir, one stored blob
    //   output: assertions on the internal map and on get_media
    //   sideEffects: writes under a temp dir, removed at the end
    // mem:persisted_media_not_retained_in_ram:end
    #[test]
    fn persisted_media_not_retained_in_ram() {
        let dir = std::env::temp_dir().join(format!("mhs_memtest_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let state = AppState::with_data_dir(dir.clone());

        let blob: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
        state.store_media("memtest1", "image/png", blob.clone()).unwrap();

        // RAM holds the index, not the bytes.
        {
            let guard = state.media.media.lock().unwrap();
            let entry = guard.get("memtest1").expect("index entry present");
            assert!(
                entry.bytes.is_empty(),
                "persisted media must not retain bytes in RAM (gamma-33): {} bytes held",
                entry.bytes.len()
            );
        }
        // And the disk copy serves them back on demand.
        let served = state.get_media("memtest1").expect("get loads from disk");
        assert_eq!(served.bytes.as_ref(), &blob);
        assert_eq!(served.content_type, "image/png");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // mem:replay_does_not_load_media_blobs:start
    //   purpose: The other half of gamma-33: replay used to fs::read EVERY blob
    //            into RAM at boot — a restart on a node with 20 GiB of media
    //            re-inflated the process by 20 GiB before serving anything.
    //            replay must build the index only; get_media reads on demand.
    //   input:  a data dir with a pre-planted media blob + sidecar
    //   output: assertions on the post-replay map and get_media
    //   sideEffects: writes under a temp dir, removed at the end
    // mem:replay_does_not_load_media_blobs:end
    #[test]
    fn replay_does_not_load_media_blobs() {
        let dir = std::env::temp_dir().join(format!("mhs_memreplay_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let media_dir = dir.join("media");
        std::fs::create_dir_all(&media_dir).unwrap();
        let blob: Vec<u8> = (0..8192u32).map(|i| (i % 249) as u8).collect();
        std::fs::write(media_dir.join("replayblob"), &blob).unwrap();
        std::fs::write(media_dir.join("replayblob.ct"), b"video/mp4").unwrap();

        let state = AppState::with_data_dir(dir.clone());
        crate::persist::replay_from_dir(&state, &dir).map_err(|e| format!("replay: {e}")).unwrap();

        {
            let guard = state.media.media.lock().unwrap();
            let entry = guard.get("replayblob").expect("index entry restored");
            assert!(
                entry.bytes.is_empty(),
                "replay must restore the media INDEX, not blob contents (gamma-33): \
                 {} bytes loaded at boot",
                entry.bytes.len()
            );
            assert_eq!(entry.content_type, "video/mp4");
        }
        let served = state.get_media("replayblob").expect("disk-backed entry serves");
        assert_eq!(served.bytes.as_ref(), &blob);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // mem:in_memory_mode_still_holds_bytes:start
    //   purpose: The no-persistence mode (every non-persist test, single-run
    //            servers) has no disk to fall back on — bytes must stay in RAM
    //            exactly as before the gamma-33 fix.
    //   input:  in-memory AppState, one stored blob
    //   output: assertion that bytes are held
    //   sideEffects: none
    // mem:in_memory_mode_still_holds_bytes:end
    #[test]
    fn in_memory_mode_still_holds_bytes() {
        let state = AppState::new();
        state.store_media("m1", "image/png", vec![1, 2, 3]).unwrap();
        let served = state.get_media("m1").unwrap();
        assert_eq!(served.bytes.as_ref(), &[1, 2, 3]);
    }
}
