use k8s_openapi::{
    api::core::v1::{ConfigMap, Node},
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
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
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use thiserror::Error;
use tracing::{debug, error, info, warn};

pub const FINALIZER_NAME: &str = "nodelabelpreserver.example.com/finalizer";
pub const CONFIGMAP_NAMESPACE: &str = "default";
const SERVICE_NAME: &str = "node-label-preserver";
const JSON_STORAGE_KEY: &str = "preserved_labels_json";
const APPLIED_FLAG_KEY: &str = "labels_applied_flag";
const REQUEUE_TIME: Duration = Duration::from_secs(10);

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
}

impl Context {
    /// Create a new Context
    pub fn new(client: Client) -> Self {
        let cm_api = Api::<ConfigMap>::namespaced(client.clone(), CONFIGMAP_NAMESPACE);
        Self { client, cm_api }
    }
}

/// Generates the expected ConfigMap name for a given node name.
fn configmap_name(node_name: &str) -> String {
    // Kubernetes names must be DNS-compatible
    format!("node-labels-{}", node_name.replace('.', "-"))
}

/// Patch the ConfigMap flag to indicate labels have been applied
async fn update_cm_flag(cm_api: &Api<ConfigMap>, cm_name: &str) -> Result<(), Error> {
    let cm_flag_patch = json!({"data": {APPLIED_FLAG_KEY: "1"}});
    cm_api
        .patch(
            cm_name,
            &PatchParams::default(),
            &Patch::Merge(&cm_flag_patch),
        )
        .await?;
    Ok(())
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
    info!("Reconciling node '{}' (Apply)", node_name);

    let node_api: Api<Node> = Api::all(ctx.client.clone());
    let mut current_labels = node.labels().clone();
    let mut labels_to_restore: BTreeMap<String, String> = BTreeMap::new();
    let mut restoration_needed = false;

    // Check ConfigMap for preserved labels and the applied flag
    let cm_name = configmap_name(&node_name);
    match ctx.cm_api.get(&cm_name).await {
        Ok(cm) => {
            if let Some(data) = &cm.data {
                if data.get(APPLIED_FLAG_KEY).map(|s| s.as_str()) == Some("0") {
                    restoration_needed = true;
                    if let Some(labels_json_str) = data.get(JSON_STORAGE_KEY) {
                        labels_to_restore =
                            serde_json::from_str(labels_json_str).map_err(Error::Serialization)?;
                    }
                }
            }
        }
        Err(kube::Error::Api(ErrorResponse { code: 404, .. })) => {}
        Err(e) => return Err(Error::Kube(e)),
    }

    // Apply labels if restoration is needed and labels differ
    let mut needs_node_patch = false;
    if restoration_needed && !labels_to_restore.is_empty() {
        for (key, value) in labels_to_restore {
            if let std::collections::btree_map::Entry::Vacant(e) = current_labels.entry(key) {
                e.insert(value);
                needs_node_patch = true;
            }
        }
    }

    // Patch Node if necessary
    if needs_node_patch {
        let node_patch = json!({"metadata": {"labels": current_labels}});
        node_api
            .patch(
                &node_name,
                &PatchParams::default(),
                &Patch::Merge(&node_patch),
            )
            .await?;
        update_cm_flag(&ctx.cm_api, &cm_name).await?;
    } else if restoration_needed {
        update_cm_flag(&ctx.cm_api, &cm_name).await?;
    }

    Ok(Action::await_change())
}

/// Handle Node Deletion
async fn cleanup_node(node: Arc<Node>, ctx: Arc<Context>) -> Result<Action> {
    let node_name = node.name_any();
    info!("Cleaning up node '{}' (Cleanup)", node_name);
    if !node
        .metadata
        .finalizers
        .as_ref()
        .is_some_and(|f| f.contains(&FINALIZER_NAME.to_string()))
    {
        return Ok(Action::await_change());
    }
    let labels_to_preserve = node.labels().clone();
    debug!(
        "Labels to preserve for node '{}': {:?}",
        node_name, labels_to_preserve
    );

    let cm_name = configmap_name(&node_name);
    let mut cm_data = BTreeMap::new();

    // Always set the applied flag to "0" during cleanup
    cm_data.insert(APPLIED_FLAG_KEY.to_string(), "0".to_string());
    if !labels_to_preserve.is_empty() {
        let labels_json =
            serde_json::to_string(&labels_to_preserve).map_err(Error::Serialization)?;
        cm_data.insert(JSON_STORAGE_KEY.to_string(), labels_json);
    }

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

/// Error policy determines action on reconciliation failure
pub fn error_policy(_node: Arc<Node>, error: &Error, _ctx: Arc<Context>) -> Action {
    error!("Reconciliation failed: {:?}", error);
    Action::requeue(REQUEUE_TIME)
}
