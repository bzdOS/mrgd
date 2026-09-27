-- aki_visibility.lua
-- DC++-style server script for matrix-hs (Lua, hot-reloadable, no Rust rebuild).
--
-- Grants the @aki:m.hubd.net account visibility into EVERY room via the
-- on_room_visible(user_id, room_id) hook wired in routes/sync.rs and
-- routes/sliding_sync.rs. Membership remains the default for everyone else;
-- this hook can only EXTEND visibility, never narrow it.
--
-- @aki is a god-view / monitoring account (e.g. the personal Matrix+NVR
-- client, an audit log, a concierge). Change the localpart below or extend the
-- predicate (per-room, per-tag) without touching the server.

local AKI = "@aki:m.hubd.net"

function on_room_visible(user_id, room_id)
    return user_id == AKI
end
