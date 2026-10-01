// START_AI_HEADER
// MODULE: couplingd/src/lib.rs
// PURPOSE: bsdOS coupling-store daemon library — Ярус 2 из SPEC_coupling_v1.
//          Экспортирует модули примитивов (session, lock, kv, queue, svc) и текстовый
//          control-протокол (proto). Модульные агенты реализуют заглушки в отдельных файлах.
// INTENT: Предоставить публичную поверхность, в которую следующие субагенты вложат
//         конкретные реализации без изменения структуры крейта.
// DEPENDENCIES: tokio, thiserror, libc; zenoh (only under the `cluster` feature — M1 is in-mem)
// PUBLIC_API: observ (enabled+emit+content_id), session, lock, kv, queue, svc, proto, os, server, consensus, jailspec, reconcile, crdt, cf_router (NodeMeta+RouteClass+Route+routes_for+select_route+desired_for+CfRouter+MockCfApi), watchdog, matrix_events, hub_bridge, barrier (Fence+ProvisionalClaim+ClaimOutcome+Policy+BarrierError+CasResult+ClaimStore+MemClaimStore+KvFencedClaimStore+claim+reconcile), barrier_net (cluster only: ClaimRequest+ClaimResponder+RoutedClaimStore), barrier_coord (cluster only: BarrierCoordinatorHandle, BarrierCoordinator::spawn), barrier_growset (cluster only: ClaimRecord+GrowSetClaimStore+LostClaim+ReconcileDriver)
// END_AI_HEADER

pub mod observ;
pub mod session;
pub mod lock;
pub mod kv;
pub mod queue;
pub mod svc;
pub mod proto;
pub mod os;
pub mod server;
pub mod consensus;
pub mod jailspec;
pub mod reconcile;
pub mod crdt;
pub mod cf_router;
pub mod watchdog;
pub mod matrix_events;
pub mod hub_bridge;
pub mod barrier;
#[cfg(feature = "cluster")]
pub mod barrier_net;
#[cfg(feature = "cluster")]
pub mod barrier_coord;
#[cfg(feature = "cluster")]
pub mod barrier_growset;
