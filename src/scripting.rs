// START_AI_HEADER
// MODULE: matrix-hs/src/scripting.rs
// PURPOSE: DC++-hub-style server scripting. Loads Lua scripts from a directory
//          at startup (hot-reloadable via load_dir) and dispatches Matrix event
//          hooks to them. The homeserver stays a generic spec-compliant server;
//          all behavioral customization (visibility overrides, command bots,
//          Zenoh bridges, automation rules) lives in Lua, not in Rust.
//
//          Current hook surface:
//            on_room_visible(user_id, room_id) -> bool
//              May a user see a room they are NOT joined to in /sync and
//              sliding_sync? Default false (standard Matrix membership).
//              Used e.g. to give a monitoring account visibility into all rooms.
//
//          Design note: Scripting holds an RwLock<Option<Arc<Lua>>> so it can
//          be Default (empty, hooks return spec defaults) and populated after
//          AppState construction in main.rs::build_state. mlua::Lua is Send+Sync.
// DEPENDENCIES: mlua (lua54)
// END_AI_HEADER

use mlua::{Function, Lua};
use std::sync::{Arc, RwLock};

/// Server-side Lua scripting engine. Lives inside AppState.
///
/// Empty by default (no hooks → standard Matrix behaviour). `load_dir` populates
/// it from `*.lua` files; the held `Lua` is swapped atomically so scripts can be
/// hot-reloaded without rebuilding the server.
pub struct Scripting {
    lua: RwLock<Option<Arc<Lua>>>,
}

impl Default for Scripting {
    fn default() -> Self {
        Self {
            lua: RwLock::new(None),
        }
    }
}

impl Scripting {
    /// Load every `*.lua` file from `dir`, executing each so they register their
    /// `on_*` globals into the shared Lua state. Replaces any previously loaded
    /// state atomically. Missing dir / unreadable files are silently skipped
    /// (the server runs fine with no scripts).
    pub fn load_dir(&self, dir: &str) {
        let lua = Arc::new(Lua::new());
        if let Ok(entries) = std::fs::read_dir(dir) {
            for ent in entries.flatten() {
                let path = ent.path();
                if path.extension().and_then(|e| e.to_str()) == Some("lua") {
                    if let Ok(src) = std::fs::read_to_string(&path) {
                        // exec registers top-level `function on_*` in _G.
                        let _ = lua.load(&src).set_name(path.to_string_lossy()).exec();
                    }
                }
            }
        }
        if let Ok(mut guard) = self.lua.write() {
            *guard = Some(lua);
        }
    }

    /// True if any Lua state currently holds the hook (used to short-circuit
    /// the lookup when scripting is disabled / unconfigured).
    fn with_hook<R>(&self, name: &str, with: impl FnOnce(&Function) -> R) -> Option<R> {
        let guard = self.lua.read().ok()?;
        let lua = guard.as_ref()?;
        let f: Function = lua.globals().get(name).ok()?;
        Some(with(&f))
    }

    /// Hook: may `user_id` see `room_id` even though they are not a joined
    /// member? Returns false (spec behaviour) when no script defines the hook
    /// or the call errors. Membership logic in sync/sliding_sync is the
    /// default; this can only EXTEND visibility, never narrow it.
    pub fn on_room_visible(&self, user_id: &str, room_id: &str) -> bool {
        self.with_hook("on_room_visible", |f| {
            f.call::<bool>((user_id, room_id)).unwrap_or(false)
        })
        .unwrap_or(false)
    }
}
