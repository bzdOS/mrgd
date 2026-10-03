// START_AI_HEADER
// MODULE: couplingd/src/cf_router.rs
// PURPOSE: cf-router — dynamic ingress reconciler (SPEC_net_v1 §3a, §3b).
//          Maintains Cloudflare DNS/LB in sync with svc-registry.
//          For svc:X → node N: if N has a public IP → direct A-record (proxied=false);
//          if N is behind NAT (no public IP, has tunnel) → proxied CNAME to CF-tunnel.
//          Multi-instance → LB pool of origins.
//          Real CF HTTP calls are ONLY compiled under the `cluster` feature (stub with TODO).
//          Default build: MockCfApi — records upserts in a Vec for tests / dry-run.
//
//          Multi-route model (§3b): a node may simultaneously carry multiple independent
//          route CLASSES — Internal (opaque mesh/private locator), Public (direct IP), and
//          Cloudflare (CF Tunnel reverse-relay). routes_for() enumerates all applicable
//          routes; desired_for/reconcile remain focused on CF DNS (Public+Cloudflare only).
//          select_route() applies a preference policy for client-side happy-eyeballs / Zenoh
//          connect.endpoints ordering.
// INTENT: Skeleton for milestone N3a (SPEC_net_v1 §3a.6) + N3b multi-route extension.
//         Bodies are complete for the pure-computation path (desired_for, routes_for,
//         select_route, reconcile).
//         HttpCfApi stub + feature gate keep the real CF dependency deferred.
//         All tests run on host; no VM, no network, no real CF calls.
// DEPENDENCIES: std, thiserror; HttpCfApi requires `cluster` feature (stub only)
// PUBLIC_API: NodeMeta, Origin, DesiredRecord, RouteClass, Route, CfError, CloudflareApi,
//             MockCfApi, HttpCfApi (cluster), CfRouter, desired_for, routes_for, select_route
// END_AI_HEADER

use std::sync::{Arc, Mutex};
use thiserror::Error;

// ── Node metadata ─────────────────────────────────────────────────────────────

// NodeMeta:start
//   purpose: Carry the per-node routing attributes that cf-router reads from
//            `bsdos/net/node/<node_id>` KV (SPEC_net_v1 §3a.2, §3b, §9).
//            `public_ip` present → ЦОД-direct; absent + `cf_tunnel_id` present → NAT-relay.
//            `internal_addr` present → node reachable inside the trusted overlay (Internal route).
//            The `internal_addr` string is opaque: it may be a carrier-grade-NAT overlay
//            address, a mesh locator such as "tcp/192.0.2.2:7447", or any other private-network
//            reachability hint — the model does not hardcode the underlying technology.
//            See SPEC_net_v1 §3b for the open Tailscale-vs-Zenoh-mesh decision.
//   input:  populated from KV watch on `bsdos/net/node/*`
//   output: consumed by desired_for(), routes_for(), and reconcile()
//   sideEffects: none (pure data)
// NodeMeta:end
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeMeta {
    /// Unique stable node identifier (e.g. TLS-certificate fingerprint, SPEC_net_v1 §8).
    pub node_id: String,
    /// Public IPv4 address of the node, or None if behind NAT.
    pub public_ip: Option<std::net::Ipv4Addr>,
    /// Cloudflare Tunnel ID for NAT nodes (format: UUID, cfargotunnel.com suffix).
    pub cf_tunnel_id: Option<String>,
    /// Opaque private/mesh locator for the Internal route class (SPEC_net_v1 §3b).
    /// Examples: "192.0.2.2:7447" (overlay range), "tcp/192.0.2.2:7447" (mesh locator).
    /// None if the node has no known internal-network address.
    pub internal_addr: Option<String>,
}

// ── LB origin ────────────────────────────────────────────────────────────────

// Origin:start
//   purpose: A single origin in a Cloudflare LB pool (SPEC_net_v1 §3.4).
//            Direct-address for ЦОД nodes; tunnel endpoint for NAT nodes.
//   input:  computed by desired_for() for multi-instance services
//   output: placed in DesiredRecord::LbPool
//   sideEffects: none (pure data)
// Origin:end
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Origin {
    /// Node identifier this origin represents.
    pub node_id: String,
    /// Resolved address/URL for this origin. For ЦОД: IP string; for NAT: tunnel URL.
    pub address: String,
}

// ── Desired DNS/LB record ─────────────────────────────────────────────────────

// DesiredRecord:start
//   purpose: Represent the exact CF state cf-router wants to assert for a hostname.
//            Three variants matching SPEC_net_v1 §3.2 table:
//              DirectA       — A-record, proxied=false (ЦОД-direct, Table row 1)
//              ProxiedTunnel — CNAME → <tunnel_id>.cfargotunnel.com, proxied=true (NAT row 2)
//              LbPool        — CF LB pool of multiple origins (§3.4 multi-instance)
//   input:  computed by desired_for() from NodeMeta
//   output: passed to CloudflareApi::upsert()
//   sideEffects: none (pure data)
// DesiredRecord:end
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DesiredRecord {
    /// ЦОД-direct: A-record pointing to the node's public IP, proxied=false.
    DirectA {
        /// Fully-qualified hostname for this service (e.g. "matrix.example.com").
        host: String,
        /// Public IPv4 of the serving node.
        ip:   std::net::Ipv4Addr,
    },
    /// NAT-relay: CNAME to Cloudflare Tunnel endpoint, proxied=true.
    ProxiedTunnel {
        /// Fully-qualified hostname for this service.
        host:      String,
        /// CF Tunnel ID; CNAME target = `<tunnel_id>.cfargotunnel.com`.
        tunnel_id: String,
    },
    /// Multi-instance: Cloudflare LB pool with multiple origins.
    LbPool {
        /// Fully-qualified hostname for the LB pool.
        host:    String,
        /// List of origins (ЦОД direct-ips or NAT tunnel-urls).
        origins: Vec<Origin>,
    },
}

// ── Errors ────────────────────────────────────────────────────────────────────

// CfError:start
//   purpose: Errors that CloudflareApi implementations can produce.
//   input:  returned by upsert() implementors
//   output: propagated through CfRouter::reconcile() → caller
//   sideEffects: none (error value)
// CfError:end
#[derive(Debug, Error)]
pub enum CfError {
    /// Node has neither a public IP nor a CF tunnel — cannot build an ingress record.
    /// cf-router must alert and skip record creation (SPEC_net_v1 §3a.6 N3d).
    #[error("node '{0}' has no public_ip and no cf_tunnel_id — cannot create ingress record")]
    NodeUnreachable(String),
    /// Real HTTP transport error (only raised by HttpCfApi, never by MockCfApi).
    #[error("cloudflare API error: {0}")]
    ApiFailure(String),
}

// ── Multi-route model (SPEC_net_v1 §3b) ──────────────────────────────────────

// RouteClass:start
//   purpose: Classify a node's ingress path by the underlying connectivity mechanism.
//            Three independent classes; a node may carry any non-empty subset simultaneously.
//            Internal  — reachable inside the trusted overlay (mesh/private network).
//            Public    — direct public IP reachable from the internet (ЦОД-node).
//            Cloudflare — public hostname via outbound CF Tunnel (NAT-node reverse-relay).
//   input:  derived by routes_for() from NodeMeta fields
//   output: tag on a Route; used by select_route() for preference ordering
//   sideEffects: none (enum value)
// RouteClass:end
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteClass {
    /// Reachable inside the trusted overlay (private/mesh address).
    /// The concrete technology is opaque: may be a Zenoh-mesh locator or a Tailscale 100.x
    /// address — see SPEC_net_v1 §3b for the open decision.
    Internal,
    /// Direct public IP reachable from the internet (ЦОД-node, white IP).
    Public,
    /// Public hostname served via an outbound Cloudflare Tunnel (NAT-node, reverse-relay).
    Cloudflare,
}

// Route:start
//   purpose: A single independent path by which a node is reachable.
//            Produced by routes_for(); consumed by select_route() and by the node-descriptor
//            publisher that writes to `bsdos/net/node/<id>` (SPEC_net_v1 §3b).
//   input:  computed from NodeMeta by routes_for()
//   output: slice passed to select_route(); ordered vec published as node descriptor
//   sideEffects: none (pure data)
// Route:end
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    /// Which class of connectivity this route represents.
    pub class: RouteClass,
    /// Opaque address/locator string for this route.
    /// Internal: private-network locator (e.g. "192.0.2.2:7447").
    /// Public:   dotted-decimal IPv4 (e.g. "203.0.113.4").
    /// Cloudflare: CF-tunnel hostname (e.g. "<tunnel_id>.cfargotunnel.com").
    pub locator: String,
    /// True only for Cloudflare routes — traffic traverses the CF edge.
    /// False for Internal and Public routes (no proxy in path).
    pub proxied: bool,
}

// routes_for:start
//   purpose: Enumerate ALL ingress routes applicable to a node.
//            Pure function; no I/O. Returns each applicable Route in preference order:
//              1. Internal  (if internal_addr is Some)
//              2. Public    (if public_ip is Some)
//              3. Cloudflare(if cf_tunnel_id is Some)
//            This ordering maps directly to Zenoh `connect.endpoints` priority:
//            internal mesh first (lowest latency), public direct second, CF relay last.
//            Returns Err(NodeUnreachable) only when NO route exists at all.
//   input:  node — NodeMeta with any combination of internal_addr / public_ip / cf_tunnel_id
//   output: Result<Vec<Route>, CfError>; Vec is non-empty on Ok, length 1–3
//   sideEffects: none
// routes_for:end
pub fn routes_for(node: &NodeMeta) -> Result<Vec<Route>, CfError> {
    let mut routes = Vec::with_capacity(3);

    // 1. Internal: opaque mesh/private locator (lowest latency, preferred when reachable).
    if let Some(ref addr) = node.internal_addr {
        routes.push(Route {
            class:   RouteClass::Internal,
            locator: addr.clone(),
            proxied: false,
        });
    }

    // 2. Public: direct IP (ЦОД-node, white IP). Second preference after Internal.
    if let Some(ip) = node.public_ip {
        routes.push(Route {
            class:   RouteClass::Public,
            locator: ip.to_string(),
            proxied: false,
        });
    }

    // 3. Cloudflare: CF-tunnel reverse-relay. Last resort (proxied, extra latency/SPOF).
    if let Some(ref tid) = node.cf_tunnel_id {
        routes.push(Route {
            class:   RouteClass::Cloudflare,
            locator: format!("{}.cfargotunnel.com", tid),
            proxied: true,
        });
    }

    if routes.is_empty() {
        return Err(CfError::NodeUnreachable(node.node_id.clone()));
    }

    Ok(routes)
}

// select_route:start
//   purpose: Pick the single best route for an immediate connection attempt, given a hint
//            about whether the caller is already on the internal mesh.
//            Policy (SPEC_net_v1 §3b happy-eyeballs):
//              - on_internal_mesh=true  → prefer Internal; fallback Public → Cloudflare
//              - on_internal_mesh=false → prefer Public; fallback Cloudflare → Internal
//            "Reachability" is policy-based only (the `on_internal_mesh` hint); no live
//            probing is done here. In production this maps to handing the ordered locators
//            from routes_for() to Zenoh's multi-endpoint `connect.endpoints`, which performs
//            actual racing/failover natively.
//   input:  routes       — slice of Route from routes_for() (must be non-empty for a result)
//           on_internal_mesh — true if the caller is a mesh-local client (e.g. another node
//                              in the Zenoh overlay); false for external/internet callers
//   output: Option<&Route> — the preferred route, or None if `routes` is empty
//   sideEffects: none (pure function)
// select_route:end
pub fn select_route<'a>(routes: &'a [Route], on_internal_mesh: bool) -> Option<&'a Route> {
    if routes.is_empty() {
        return None;
    }

    // Define the preference order for each caller context.
    let preference: &[RouteClass] = if on_internal_mesh {
        &[RouteClass::Internal, RouteClass::Public, RouteClass::Cloudflare]
    } else {
        &[RouteClass::Public, RouteClass::Cloudflare, RouteClass::Internal]
    };

    for preferred_class in preference {
        if let Some(route) = routes.iter().find(|r| &r.class == preferred_class) {
            return Some(route);
        }
    }

    // Fallback: no class matched the preference list — return first available.
    routes.first()
}

// ── CloudflareApi trait ───────────────────────────────────────────────────────

// CloudflareApi:start
//   purpose: Abstract the Cloudflare DNS/LB mutation surface so that reconcile()
//            is testable without real HTTP calls.
//            Implementations: MockCfApi (default, tests), HttpCfApi (cluster feature, stub).
//   input:  rec — the desired DNS/LB record to assert idempotently
//   output: Result<(), CfError>; Ok if upsert succeeded or was a no-op
//   sideEffects: [MockCfApi] appends to internal Vec; [HttpCfApi] would issue HTTP POST/PUT
// CloudflareApi:end
pub trait CloudflareApi: Send + Sync {
    // upsert:start
    //   purpose: Assert the desired record in CF — create if absent, update if changed.
    //            Idempotent: calling twice with the same record must not error.
    //   input:  rec — DesiredRecord to upsert
    //   output: Result<(), CfError>
    //   sideEffects: implementation-defined (see struct docs)
    // upsert:end
    fn upsert(&self, rec: &DesiredRecord) -> Result<(), CfError>;
}

// ── MockCfApi — for tests and dry-run ─────────────────────────────────────────

// MockCfApi:start
//   purpose: In-memory CloudflareApi for host tests and dry-run mode.
//            Records every upsert call so tests can assert the correct record was computed
//            and sent without touching the real CF API or requiring network access.
//   input:  upsert(rec) — appends a clone of rec to inner Vec under Mutex
//   output: always Ok(())
//   sideEffects: writes to self.inner (Arc<Mutex<Vec<DesiredRecord>>>)
// MockCfApi:end
#[derive(Clone, Default)]
pub struct MockCfApi {
    inner: Arc<Mutex<Vec<DesiredRecord>>>,
}

impl MockCfApi {
    // new:start
    //   purpose: Construct an empty MockCfApi with no recorded upserts.
    //   input:  none
    //   output: MockCfApi
    //   sideEffects: allocates Arc<Mutex<Vec>>
    // new:end
    pub fn new() -> Self {
        Self::default()
    }

    // recorded:start
    //   purpose: Return a snapshot of all upserts recorded so far.
    //            Used by tests to assert the sequence of records cf-router emitted.
    //   input:  none (reads from self.inner)
    //   output: Vec<DesiredRecord> — cloned snapshot; empty if no upserts yet
    //   sideEffects: Mutex read lock
    // recorded:end
    pub fn recorded(&self) -> Vec<DesiredRecord> {
        self.inner
            .lock()
            .expect("MockCfApi mutex must not be poisoned")
            .clone()
    }
}

impl CloudflareApi for MockCfApi {
    fn upsert(&self, rec: &DesiredRecord) -> Result<(), CfError> {
        // Log the upsert so dry-run callers and tests can observe it.
        eprintln!("[mock-cf] upsert {:?}", rec);
        self.inner
            .lock()
            .expect("MockCfApi mutex must not be poisoned")
            .push(rec.clone());
        Ok(())
    }
}

// ── HttpCfApi — real HTTP, compiled only under `cluster` feature ───────────────

// HttpCfApi:start
//   purpose: Real Cloudflare API client — issues HTTP PUT/POST to CF DNS/LB.
//            STUB ONLY: body is TODO; will use reqwest (or ureq) with a scoped
//            CF API token (Zone:DNS:Edit) from coupling-store secrets.
//            Compiled ONLY under `cluster` feature to keep the default build light
//            (no HTTP client in the dependency graph for M1/M2).
//   input:  upsert(rec) — TODO: translate DesiredRecord to CF API JSON payload, POST
//   output: TODO: Result<(), CfError> from CF HTTP response
//   sideEffects: TODO: real HTTPS outbound to api.cloudflare.com — NEVER called in tests
// HttpCfApi:end
#[cfg(feature = "cluster")]
pub struct HttpCfApi {
    // TODO(N3a): zone_id, api_token, reqwest::blocking::Client or async client
    _zone_id:  String,
    _api_token: String,
}

#[cfg(feature = "cluster")]
impl HttpCfApi {
    // new:start
    //   purpose: Construct HttpCfApi with scoped CF API credentials.
    //            Credentials must come from coupling-store secret (never hardcoded).
    //   input:  zone_id — CF Zone ID string; api_token — Zone:DNS:Edit scoped token
    //   output: HttpCfApi
    //   sideEffects: none (no network at construction time)
    // new:end
    pub fn new(zone_id: String, api_token: String) -> Self {
        Self {
            _zone_id: zone_id,
            _api_token: api_token,
        }
    }
}

#[cfg(feature = "cluster")]
impl CloudflareApi for HttpCfApi {
    fn upsert(&self, rec: &DesiredRecord) -> Result<(), CfError> {
        // TODO(N3a): Translate DesiredRecord to CF API v4 DNS record JSON:
        //   DirectA       → PUT /zones/{zone_id}/dns_records  type=A, proxied=false
        //   ProxiedTunnel → PUT /zones/{zone_id}/dns_records  type=CNAME,
        //                       content=<tunnel_id>.cfargotunnel.com, proxied=true
        //   LbPool        → POST /zones/{zone_id}/load_balancers  origins=[...]
        // Use Authorization: Bearer {api_token} header.
        // Parse CF API error response; map to CfError::ApiFailure(msg).
        let _ = rec; // suppress unused warning in stub
        Err(CfError::ApiFailure(
            "HttpCfApi::upsert not implemented — TODO(N3a)".to_string(),
        ))
    }
}

// ── Pure computation: desired_for ─────────────────────────────────────────────

// desired_for:start
//   purpose: Compute the desired Cloudflare DNS record for a single-instance service
//            hosted on the given node, given the node's routing metadata.
//            Pure function — no side effects, no I/O, trivially testable.
//            Scope: CF DNS only (Public + Cloudflare route classes); the Internal route
//            is NOT a CF DNS record — it is distributed via the node descriptor KV key
//            (`bsdos/net/node/<id>`) for mesh-local clients (SPEC_net_v1 §3b).
//            Relationship to routes_for(): desired_for is equivalent to filtering
//            routes_for(node) to Public|Cloudflare and mapping to DesiredRecord.
//            Algorithm (SPEC_net_v1 §3a.2):
//              1. public_ip = Some(ip)                       → DirectA (ЦОД-direct)
//              2. public_ip = None, cf_tunnel_id = Some(tid) → ProxiedTunnel (NAT-relay)
//              3. neither public_ip nor cf_tunnel_id         → Err(NodeUnreachable)
//            (Internal-only nodes return NodeUnreachable because they have no CF DNS record.)
//   input:  svc_host — FQDN for this service (e.g. "matrix.example.com");
//           node — NodeMeta carrying public_ip and cf_tunnel_id
//   output: Result<DesiredRecord, CfError>
//   sideEffects: none
// desired_for:end
pub fn desired_for(svc_host: &str, node: &NodeMeta) -> Result<DesiredRecord, CfError> {
    match (&node.public_ip, &node.cf_tunnel_id) {
        // Case 1: ЦОД-node with a public IP → direct A-record, no CF proxy in path.
        (Some(ip), _) => Ok(DesiredRecord::DirectA {
            host: svc_host.to_string(),
            ip:   *ip,
        }),
        // Case 2: NAT-node with a CF tunnel → proxied CNAME to cfargotunnel.com.
        (None, Some(tid)) => Ok(DesiredRecord::ProxiedTunnel {
            host:      svc_host.to_string(),
            tunnel_id: tid.clone(),
        }),
        // Case 3: neither → node is unreachable from external internet — alert, no record.
        (None, None) => Err(CfError::NodeUnreachable(node.node_id.clone())),
    }
}

// ── CfRouter ─────────────────────────────────────────────────────────────────

// CfRouter:start
//   purpose: Stateless reconciler — asserts desired CF state for a svc:host/node pair.
//            Holds an Arc<dyn CloudflareApi> so that MockCfApi and HttpCfApi are
//            interchangeable without recompiling the router logic.
//            Multiple CfRouter instances are safe (CF API is idempotent per §3a.3).
// CfRouter:end
pub struct CfRouter {
    api: Arc<dyn CloudflareApi>,
}

impl CfRouter {
    // new:start
    //   purpose: Construct a CfRouter backed by the given CloudflareApi implementation.
    //   input:  api — Arc<dyn CloudflareApi>; typically MockCfApi or HttpCfApi
    //   output: CfRouter
    //   sideEffects: none
    // new:end
    pub fn new(api: Arc<dyn CloudflareApi>) -> Self {
        Self { api }
    }

    // reconcile:start
    //   purpose: Bring CF into the desired state for `svc_host` when it is hosted on `node`.
    //            Computes desired_for(svc_host, node) → calls api.upsert(desired).
    //            Idempotent: re-calling with the same arguments is safe (CF API + MockCfApi
    //            both tolerate duplicate upserts).
    //            On NodeUnreachable: returns Err without calling upsert, so callers can log/alert
    //            (N3d milestone in §3a.6).
    //   input:  svc_host — FQDN for the service (e.g. "matrix.example.com");
    //           node — NodeMeta of the node currently hosting the service
    //   output: Result<(), CfError>; Err(NodeUnreachable) if node has no ingress path
    //   sideEffects: calls self.api.upsert() which appends to MockCfApi or issues HTTP
    // reconcile:end
    pub fn reconcile(&self, svc_host: &str, node: &NodeMeta) -> Result<(), CfError> {
        let desired = desired_for(svc_host, node)?;
        eprintln!(
            "[cf-router] reconcile {} on node {} → {:?}",
            svc_host, node.node_id, desired
        );
        self.api.upsert(&desired)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    // ── desired_for: ЦОД-node → DirectA ──────────────────────────────────────

    #[test]
    fn desired_for_datacenter_node_returns_direct_a() {
        let node = NodeMeta {
            node_id:       "dc-fra-01".to_string(),
            public_ip:     Some(Ipv4Addr::new(203, 0, 113, 4)),
            cf_tunnel_id:  None,
            internal_addr: None,
        };
        let rec = desired_for("matrix.example.com", &node)
            .expect("ЦОД node must produce DirectA without error");
        assert_eq!(
            rec,
            DesiredRecord::DirectA {
                host: "matrix.example.com".to_string(),
                ip:   Ipv4Addr::new(203, 0, 113, 4),
            },
            "DirectA must carry the node's public IP"
        );
    }

    // ── desired_for: NAT-node (tunnel present) → ProxiedTunnel ───────────────

    #[test]
    fn desired_for_nat_node_with_tunnel_returns_proxied_cname() {
        let node = NodeMeta {
            node_id:       "nat-home-01".to_string(),
            public_ip:     None,
            cf_tunnel_id:  Some("abc123def456".to_string()),
            internal_addr: None,
        };
        let rec = desired_for("synapse.example.com", &node)
            .expect("NAT node with tunnel must produce ProxiedTunnel without error");
        assert_eq!(
            rec,
            DesiredRecord::ProxiedTunnel {
                host:      "synapse.example.com".to_string(),
                tunnel_id: "abc123def456".to_string(),
            },
            "ProxiedTunnel must carry the tunnel_id from NodeMeta"
        );
    }

    // ── desired_for: node with both public_ip and tunnel → DirectA preferred ──

    #[test]
    fn desired_for_node_with_both_prefers_direct_a() {
        // If a node somehow has both (e.g. ЦОД with tunnel for redundancy),
        // direct path wins (public_ip match arm is first).
        let node = NodeMeta {
            node_id:       "dc-both".to_string(),
            public_ip:     Some(Ipv4Addr::new(203, 0, 113, 8)),
            cf_tunnel_id:  Some("should-be-ignored".to_string()),
            internal_addr: None,
        };
        let rec = desired_for("api.example.com", &node)
            .expect("node with both must produce DirectA");
        assert!(
            matches!(rec, DesiredRecord::DirectA { .. }),
            "DirectA must be preferred over ProxiedTunnel when public_ip is present"
        );
    }

    // ── desired_for: node without IP or tunnel → NodeUnreachable ─────────────

    #[test]
    fn desired_for_unreachable_node_returns_error() {
        let node = NodeMeta {
            node_id:       "ghost-node".to_string(),
            public_ip:     None,
            cf_tunnel_id:  None,
            internal_addr: None,
        };
        let err = desired_for("svc.example.com", &node)
            .expect_err("unreachable node must return NodeUnreachable error");
        assert!(
            matches!(err, CfError::NodeUnreachable(ref id) if id == "ghost-node"),
            "expected NodeUnreachable(ghost-node), got {:?}", err
        );
    }

    // ── reconcile via MockCfApi → correct upsert recorded ─────────────────────

    #[test]
    fn reconcile_datacenter_node_records_direct_a_in_mock() {
        let mock = Arc::new(MockCfApi::new());
        let router = CfRouter::new(mock.clone());

        let node = NodeMeta {
            node_id:       "dc-ams-01".to_string(),
            public_ip:     Some(Ipv4Addr::new(10, 0, 0, 1)),
            cf_tunnel_id:  None,
            internal_addr: None,
        };

        router
            .reconcile("app.example.com", &node)
            .expect("reconcile of ЦОД node must succeed");

        let calls = mock.recorded();
        assert_eq!(calls.len(), 1, "exactly one upsert must be recorded");
        assert_eq!(
            calls[0],
            DesiredRecord::DirectA {
                host: "app.example.com".to_string(),
                ip:   Ipv4Addr::new(10, 0, 0, 1),
            }
        );
    }

    // ── reconcile via MockCfApi → NAT node records ProxiedTunnel ─────────────

    #[test]
    fn reconcile_nat_node_records_proxied_tunnel_in_mock() {
        let mock = Arc::new(MockCfApi::new());
        let router = CfRouter::new(mock.clone());

        let node = NodeMeta {
            node_id:       "nat-london-01".to_string(),
            public_ip:     None,
            cf_tunnel_id:  Some("tunnel-uuid-xyz".to_string()),
            internal_addr: None,
        };

        router
            .reconcile("chat.example.com", &node)
            .expect("reconcile of NAT node must succeed");

        let calls = mock.recorded();
        assert_eq!(calls.len(), 1, "exactly one upsert must be recorded");
        assert_eq!(
            calls[0],
            DesiredRecord::ProxiedTunnel {
                host:      "chat.example.com".to_string(),
                tunnel_id: "tunnel-uuid-xyz".to_string(),
            }
        );
    }

    // ── service migration: ЦОД → NAT node changes desired record ─────────────

    #[test]
    fn reconcile_migration_datacenter_to_nat_changes_desired_record() {
        let mock = Arc::new(MockCfApi::new());
        let router = CfRouter::new(mock.clone());

        // First: service runs on ЦОД node.
        let dc_node = NodeMeta {
            node_id:       "dc-hel-01".to_string(),
            public_ip:     Some(Ipv4Addr::new(192, 168, 1, 10)),
            cf_tunnel_id:  None,
            internal_addr: None,
        };
        router
            .reconcile("pg.example.com", &dc_node)
            .expect("first reconcile must succeed");

        // Then: service migrates (failover) to NAT node.
        let nat_node = NodeMeta {
            node_id:       "nat-user-pc".to_string(),
            public_ip:     None,
            cf_tunnel_id:  Some("tunnel-failover-99".to_string()),
            internal_addr: None,
        };
        router
            .reconcile("pg.example.com", &nat_node)
            .expect("second reconcile must succeed");

        let calls = mock.recorded();
        assert_eq!(calls.len(), 2, "two upserts: one per reconcile call");

        // First call was DirectA.
        assert!(
            matches!(&calls[0], DesiredRecord::DirectA { ip, .. } if *ip == Ipv4Addr::new(192, 168, 1, 10)),
            "first upsert must be DirectA with ЦОД IP"
        );

        // Second call is ProxiedTunnel — desired state changed after migration.
        assert!(
            matches!(&calls[1], DesiredRecord::ProxiedTunnel { tunnel_id, .. } if tunnel_id == "tunnel-failover-99"),
            "second upsert must be ProxiedTunnel for NAT failover node"
        );
    }

    // ── reconcile unreachable node → Err, no upsert recorded ─────────────────

    #[test]
    fn reconcile_unreachable_node_returns_err_and_does_not_call_upsert() {
        let mock = Arc::new(MockCfApi::new());
        let router = CfRouter::new(mock.clone());

        let node = NodeMeta {
            node_id:       "isolated".to_string(),
            public_ip:     None,
            cf_tunnel_id:  None,
            internal_addr: None,
        };

        let err = router
            .reconcile("svc.example.com", &node)
            .expect_err("reconcile of unreachable node must fail");
        assert!(
            matches!(err, CfError::NodeUnreachable(_)),
            "expected NodeUnreachable, got {:?}", err
        );
        assert!(
            mock.recorded().is_empty(),
            "no upsert must be called for unreachable nodes (N3d)"
        );
    }

    // ── routes_for: ЦОД node (public + internal) → [Internal, Public] ─────────

    #[test]
    fn routes_for_datacenter_node_returns_internal_then_public() {
        let node = NodeMeta {
            node_id:       "dc-fra-01".to_string(),
            public_ip:     Some(Ipv4Addr::new(203, 0, 113, 4)),
            cf_tunnel_id:  None,
            internal_addr: Some("192.0.2.2:7447".to_string()),
        };
        let routes = routes_for(&node).expect("DC node with public+internal must succeed");
        assert_eq!(routes.len(), 2, "DC node: exactly two routes (Internal + Public)");
        assert_eq!(routes[0].class, RouteClass::Internal, "first must be Internal");
        assert_eq!(routes[0].proxied, false, "Internal must not be proxied");
        assert_eq!(routes[0].locator, "192.0.2.2:7447");
        assert_eq!(routes[1].class, RouteClass::Public, "second must be Public");
        assert_eq!(routes[1].proxied, false, "Public must not be proxied");
        assert_eq!(routes[1].locator, "203.0.113.4");
    }

    // ── routes_for: NAT node (tunnel + internal) → [Internal, Cloudflare] ─────

    #[test]
    fn routes_for_nat_node_returns_internal_then_cloudflare() {
        let node = NodeMeta {
            node_id:       "nat-home-01".to_string(),
            public_ip:     None,
            cf_tunnel_id:  Some("abc123def456".to_string()),
            internal_addr: Some("192.0.2.5:7447".to_string()),
        };
        let routes = routes_for(&node).expect("NAT node with tunnel+internal must succeed");
        assert_eq!(routes.len(), 2, "NAT node: exactly two routes (Internal + Cloudflare)");
        assert_eq!(routes[0].class, RouteClass::Internal, "first must be Internal");
        assert_eq!(routes[1].class, RouteClass::Cloudflare, "second must be Cloudflare");
        assert_eq!(routes[1].proxied, true, "Cloudflare must be proxied");
        assert_eq!(routes[1].locator, "abc123def456.cfargotunnel.com");
    }

    // ── routes_for: all three fields set → [Internal, Public, Cloudflare] ─────

    #[test]
    fn routes_for_all_three_returns_all_three_in_order() {
        let node = NodeMeta {
            node_id:       "full-node".to_string(),
            public_ip:     Some(Ipv4Addr::new(203, 0, 113, 9)),
            cf_tunnel_id:  Some("tid-xyz".to_string()),
            internal_addr: Some("198.51.100.1:7447".to_string()),
        };
        let routes = routes_for(&node).expect("node with all three must succeed");
        assert_eq!(routes.len(), 3, "expected exactly three routes");
        assert_eq!(routes[0].class, RouteClass::Internal);
        assert_eq!(routes[1].class, RouteClass::Public);
        assert_eq!(routes[2].class, RouteClass::Cloudflare);
    }

    // ── routes_for: internal-only node → [Internal] ───────────────────────────

    #[test]
    fn routes_for_internal_only_node_returns_single_internal() {
        let node = NodeMeta {
            node_id:       "mesh-only-01".to_string(),
            public_ip:     None,
            cf_tunnel_id:  None,
            internal_addr: Some("203.0.113.3:7447".to_string()),
        };
        let routes = routes_for(&node).expect("internal-only node must succeed");
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].class, RouteClass::Internal);
        assert_eq!(routes[0].proxied, false);
    }

    // ── routes_for: no routes → Err(NodeUnreachable) ─────────────────────────

    #[test]
    fn routes_for_empty_node_returns_node_unreachable() {
        let node = NodeMeta {
            node_id:       "ghost".to_string(),
            public_ip:     None,
            cf_tunnel_id:  None,
            internal_addr: None,
        };
        let err = routes_for(&node).expect_err("empty node must return NodeUnreachable");
        assert!(
            matches!(err, CfError::NodeUnreachable(ref id) if id == "ghost"),
            "expected NodeUnreachable(ghost), got {:?}", err
        );
    }

    // ── select_route: on_internal_mesh=true → prefers Internal ───────────────

    #[test]
    fn select_route_on_mesh_prefers_internal() {
        let node = NodeMeta {
            node_id:       "dc-node".to_string(),
            public_ip:     Some(Ipv4Addr::new(203, 0, 113, 11)),
            cf_tunnel_id:  Some("t-id".to_string()),
            internal_addr: Some("192.0.2.1:7447".to_string()),
        };
        let routes = routes_for(&node).expect("routes must be non-empty");
        let chosen = select_route(&routes, true).expect("must choose a route");
        assert_eq!(chosen.class, RouteClass::Internal,
            "on_internal_mesh=true must prefer Internal");
    }

    // ── select_route: on_internal_mesh=false → prefers Public ────────────────

    #[test]
    fn select_route_off_mesh_prefers_public() {
        let node = NodeMeta {
            node_id:       "dc-node-2".to_string(),
            public_ip:     Some(Ipv4Addr::new(203, 0, 113, 22)),
            cf_tunnel_id:  Some("t-id-2".to_string()),
            internal_addr: Some("192.0.2.2:7447".to_string()),
        };
        let routes = routes_for(&node).expect("routes must be non-empty");
        let chosen = select_route(&routes, false).expect("must choose a route");
        assert_eq!(chosen.class, RouteClass::Public,
            "on_internal_mesh=false must prefer Public over CF and Internal");
    }

    // ── select_route: off-mesh, no Public → falls back to Cloudflare ─────────

    #[test]
    fn select_route_off_mesh_no_public_falls_back_to_cloudflare() {
        let node = NodeMeta {
            node_id:       "nat-node".to_string(),
            public_ip:     None,
            cf_tunnel_id:  Some("t-nat".to_string()),
            internal_addr: Some("192.0.2.3:7447".to_string()),
        };
        let routes = routes_for(&node).expect("routes must be non-empty");
        let chosen = select_route(&routes, false).expect("must choose a route");
        assert_eq!(chosen.class, RouteClass::Cloudflare,
            "off-mesh without Public must fall back to Cloudflare");
    }

    // ── select_route: empty slice → None ─────────────────────────────────────

    #[test]
    fn select_route_empty_slice_returns_none() {
        assert!(
            select_route(&[], true).is_none(),
            "empty route slice must yield None"
        );
    }
}
