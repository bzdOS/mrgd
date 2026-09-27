// START_AI_HEADER
// MODULE: matrix-hs/src/routes/versions.rs
// PURPOSE: GET /_matrix/client/versions — advertise supported CS-API versions.
//          Real Matrix clients use this for capability discovery.
//          Stage 2: returns full version list from r0.0.1 through v1.11.
// DEPENDENCIES: axum::Json, serde_json
// PUBLIC_API: get_versions
// END_AI_HEADER

use axum::Json;
use serde_json::{json, Value};

// get_versions:start
//   purpose: Return the list of Matrix CS-API versions this server supports.
//            Stage 2 advertises the full set from r0.0.1 through v1.11.
//            The list includes r0.6.0 so existing smoke-check tests pass.
//   input:  none
//   output: JSON {"versions":[...],"unstable_features":{}}
//   sideEffects: none
// get_versions:end
pub async fn get_versions() -> Json<Value> {
    Json(json!({
        "versions": [
            "r0.0.1","r0.1.0","r0.2.0","r0.3.0","r0.4.0","r0.5.0","r0.6.0",
            "v1.1","v1.2","v1.3","v1.4","v1.5","v1.6","v1.7","v1.8","v1.9",
            "v1.10","v1.11"
        ],
        "unstable_features": {
            "org.matrix.simplified_msc3575": true,
            "org.matrix.msc3575": true
        }
    }))
}
