use futures::stream::StreamExt;
use k8s_openapi::api::core::v1::Node;
use kube::{
    api::Api,
    runtime::{controller::Controller, watcher},
    Client,
};
use label_preserver::{error_policy, reconcile, Context, CONFIGMAP_NAMESPACE, FINALIZER_NAME};
use std::sync::Arc;
use tracing::{info, warn};
use tracing_subscriber::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter = tracing_subscriber::filter::Targets::new()
        .with_target("label_preserver", tracing::Level::DEBUG);
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(filter)
        .init();

    let client = Client::try_default().await?;
    info!("Kubernetes client initialized successfully.");

    let node_api: Api<Node> = Api::all(client.clone());
    let context = Arc::new(Context::new(client.clone()));

    info!("Starting Node Label Preserver controller...");
    info!("Watching Nodes cluster-wide.");
    info!("Using finalizer: {}", FINALIZER_NAME);
    info!(
        "Storing label backups in ConfigMaps within namespace: {}",
        CONFIGMAP_NAMESPACE
    );

    Controller::new(node_api, watcher::Config::default())
        .run(reconcile, error_policy, context)
        .for_each(|res| async move {
            match res {
                Ok((obj, _action)) => info!("Reconciled Node '{}'", obj.name),
                Err(e) => warn!("Reconciliation error: {:?}", e),
            }
        })
        .await;

    info!("Controller finished.");
    Ok(())
}
