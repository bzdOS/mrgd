// START_AI_HEADER
// MODULE: matrix-hs/src/routes/mod.rs
// PURPOSE: Re-export all handler modules.
//          Stage 1: versions, login, rooms, send, sync.
//          Stage 2: discovery, account, stubs, room_state.
//          Stage 2.5: register (UIA account creation + availability check).
//          Stage 3 OTK: keys (upload/query/claim — OWNERSHIP-PARTITION exactly-once barrier).
//          Ephemeral EDUs: ephemeral (typing/receipt/read_markers — real implementation,
//          moved out of stubs.rs).
// END_AI_HEADER

pub mod account;
pub mod discovery;
pub mod login;
pub mod room_state;
pub mod rooms;
pub mod send;
pub mod sliding_sync;
pub mod sync;
pub mod versions;
// stubs.rs removed: every endpoint it once held became a real implementation
// (ephemeral.rs, to_device.rs, keys.rs, pushers.rs, voip.rs). Its last stub
// (voip/turnServer) moved to routes::voip with a real TURN credential handler.
pub mod account_data;
pub mod account_password;
pub mod ephemeral;
pub mod keys;
pub mod media;
pub mod push;
pub mod pushers;
pub mod redact;
pub mod register;
pub mod room_keys;
pub mod to_device;
pub mod voip;
