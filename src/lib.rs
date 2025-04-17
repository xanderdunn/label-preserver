use k8s_openapi::{
    api::core::v1::{ConfigMap, Node},
    apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time},
};
use kube::{
    api::{Api, Patch, PatchParams, ResourceExt},
    error::ErrorResponse,
    runtime::{
        controller::Action,
        finalizer::{finalizer, Event as FinalizerEvent},
    },
    Client,
};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};
use thiserror::Error;
use tracing::{debug, error, info, warn};

// TODO: Make these configurable
pub const CONFIGMAP_NAMESPACE: &str = "default";
const FINALIZER_NAME: &str = "nodelabelpreserver.example.com/finalizer";
const SERVICE_NAME: &str = "node-label-preserver";
const JSON_STORAGE_KEY: &str = "preserved_labels_json";
/// 1 after annotations are restored, otherwise the key is missing from the Node
const RESTORED_ANNOTATION_KEY: &str = "nodelabelpreserver.example.com/labels-restored";
const REQUEUE_TIME: Duration = Duration::from_secs(2);
const MAX_RETRY_TIME: Duration = Duration::from_secs(3600);

#[derive(Debug, Error)]
pub enum Error {
    #[error("Failed to get node name: {0:?}")]
    MissingNodeName(Node),
    #[error("Kubernetes API error: {0}")]
    Kube(#[from] kube::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Finalizer error: {0}")]
    Finalizer(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Passed to the reconciler
pub struct Context {
    client: Client,
    cm_api: Api<ConfigMap>,
    attempt: AtomicU32,
}

impl Context {
    /// Create a new Context
    pub fn new(client: Client) -> Self {
        let cm_api = Api::<ConfigMap>::namespaced(client.clone(), CONFIGMAP_NAMESPACE);
        Self {
            client,
            cm_api,
            attempt: AtomicU32::new(0),
        }
    }
}

/// Generates the expected ConfigMap name for a given node name.
/// We hash the node name to a fixed length to ensure our ConfigMap
/// name is not longer than Kubernetes' key character limit.
fn configmap_name(node_name: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(node_name.as_bytes());
    let full_hash = hasher.finalize();
    let hex_encoded_hash = hex::encode(full_hash);
    // The resulting name ("node-labels-" + 64 hex chars)
    format!("node-labels-{}", hex_encoded_hash)
}

// Action to take on Node events
pub async fn reconcile(node: Arc<Node>, ctx: Arc<Context>) -> Result<Action> {
    let node_name = node
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| Error::MissingNodeName(node.as_ref().clone()))?
        .to_string();
    let node_api: Api<Node> = Api::all(ctx.client.clone());

    finalizer(&node_api, FINALIZER_NAME, node, |event| async {
        match event {
            FinalizerEvent::Apply(node) => apply_node(node, ctx.clone()).await,
            FinalizerEvent::Cleanup(node) => cleanup_node(node, ctx.clone()).await,
        }
    })
    .await
    .map_err(|e| {
        warn!("Finalizer error for node {}: {:?}", node_name, e);
        Error::Finalizer(e.to_string())
    })
}

/// Handle Node Creation
async fn apply_node(node: Arc<Node>, ctx: Arc<Context>) -> Result<Action> {
    let node_name = node.name_any();
    if node.annotations().contains_key(RESTORED_ANNOTATION_KEY) {
        return Ok(Action::await_change());
    }
    info!("Reconciling node '{}' (Apply)", node_name);

    let node_api: Api<Node> = Api::all(ctx.client.clone());
    let mut current_labels = node.labels().clone();
    let mut labels_to_restore: BTreeMap<String, String> = BTreeMap::new();

    // Check ConfigMap for preserved labels
    let cm_name = configmap_name(&node_name);
    match ctx.cm_api.get(&cm_name).await {
        Ok(cm) => {
            if let Some(data) = &cm.data {
                if let Some(labels_json_str) = data.get(JSON_STORAGE_KEY) {
                    labels_to_restore =
                        serde_json::from_str(labels_json_str).map_err(Error::Serialization)?;
                }
            }
        }
        Err(kube::Error::Api(ErrorResponse { code: 404, .. })) => {}
        Err(e) => return Err(Error::Kube(e)),
    }

    // Apply labels if they differ
    if !labels_to_restore.is_empty() {
        for (key, value) in labels_to_restore {
            // Merge strategy: only apply if key is not already present
            if let std::collections::btree_map::Entry::Vacant(entry) = current_labels.entry(key) {
                entry.insert(value);
            }
        }
    }

    // Patch node
    let mut annotations_to_apply = BTreeMap::new();
    annotations_to_apply.insert(RESTORED_ANNOTATION_KEY.to_string(), "1".to_string());
    let apply_payload = Node {
        metadata: ObjectMeta {
            name: Some(node_name.clone()),
            labels: Some(current_labels),
            annotations: Some(annotations_to_apply),
            ..Default::default()
        },
        spec: None,
        status: None,
    };
    let patch_params = PatchParams::apply(SERVICE_NAME).force();
    node_api
        .patch(&node_name, &patch_params, &Patch::Apply(&apply_payload))
        .await
        .map_err(Error::Kube)?;

    Ok(Action::await_change())
}

/// Handle Node Deletion
async fn cleanup_node(node: Arc<Node>, ctx: Arc<Context>) -> Result<Action> {
    let node_name = node.name_any();
    info!("Cleaning up node '{}' (Cleanup)", node_name);

    // Check if deletion has been pending for too long.
    // This check is to prevent our finalizer from indefinitely preventing a resource from
    // being deleted if our cleanup is failing in a loop.
    if let Some(Time(deletion_time)) = node.metadata.deletion_timestamp {
        let current_time = SystemTime::now();
        let deletion_system_time: SystemTime = deletion_time.into();
        if current_time
            .duration_since(deletion_system_time)
            .unwrap_or_default()
            > MAX_RETRY_TIME
        {
            warn!(
                "Node '{}' termination cleanup failed for over {}. Forcing finalizer removal.",
                node_name,
                MAX_RETRY_TIME.as_secs()
            );
            return Ok(Action::await_change());
        }
    }

    let labels_to_preserve = node.labels().clone();
    debug!(
        "Labels to preserve for node '{}': {:?}",
        node_name, labels_to_preserve
    );

    let cm_name = configmap_name(&node_name);
    let mut cm_data = BTreeMap::new();

    if !labels_to_preserve.is_empty() {
        let labels_json =
            serde_json::to_string(&labels_to_preserve).map_err(Error::Serialization)?;
        cm_data.insert(JSON_STORAGE_KEY.to_string(), labels_json);
    }
    // We write a ConfigMap with no data when there are no label to preserve
    // because otherwise we may keep around outdated labels from a previous
    // node deletion.
    let cm = ConfigMap {
        metadata: ObjectMeta {
            name: Some(cm_name.clone()),
            namespace: Some(CONFIGMAP_NAMESPACE.to_string()),
            ..Default::default()
        },
        data: Some(cm_data),
        binary_data: None,
        immutable: None,
    };

    let patch_params = PatchParams::apply(SERVICE_NAME).force();
    ctx.cm_api
        .patch(&cm_name, &patch_params, &Patch::Apply(&cm))
        .await
        .map_err(Error::Kube)?;

    Ok(Action::await_change())
}

/// Exponential backoff on error
pub fn error_policy(_node: Arc<Node>, error: &Error, ctx: Arc<Context>) -> Action {
    error!("Reconciliation failed: {:?}", error);
    let attempt = ctx.attempt.fetch_add(1, Ordering::SeqCst) + 1;
    let base_secs = REQUEUE_TIME.as_secs();
    let max_secs = MAX_RETRY_TIME.as_secs();
    // 2**attempt
    let factor = 2u64.checked_pow(attempt).unwrap_or(u64::MAX);
    let delay_s = base_secs.saturating_mul(factor).min(max_secs);
    Action::requeue(Duration::from_secs(delay_s))
}
