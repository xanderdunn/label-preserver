#[cfg(test)]
mod tests {
    use k8s_openapi::api::core::v1::Node;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use kube::api::{PartialObjectMetaExt, Patch, PatchParams, PostParams};
    use kube::{api::Api, Client};
    use rand::{distr::Alphanumeric, rng, Rng};
    use serde_json::json;
    use std::collections::BTreeMap;

    /// Set label values on a node.
    pub async fn set_node_labels(
        client: &Client,
        node_name: &str,
        labels: &BTreeMap<String, String>,
    ) -> Result<(), anyhow::Error> {
        let nodes = Api::<Node>::all(client.clone());

        let patch_params = PatchParams::default();

        let metadata = ObjectMeta {
            labels: Some(labels.clone()),
            ..Default::default()
        }
        .into_request_partial::<Node>();

        nodes
            .patch_metadata(node_name, &patch_params, &Patch::Merge(metadata))
            .await?;
        Ok(())
    }

    /// Remove the label on a node.
    pub async fn delete_node_label(
        client: &Client,
        node_name: &str,
        label_key: &str,
    ) -> Result<(), anyhow::Error> {
        let nodes: Api<Node> = Api::all(client.clone());

        let patch_params = PatchParams::default();

        let patch_payload = json!({
            "metadata": {
                "labels": {
                    label_key: serde_json::Value::Null
                }
            }
        });

        let patch = Patch::Strategic(patch_payload);

        nodes.patch(node_name, &patch_params, &patch).await?;
        Ok(())
    }

    /// Add a label or update its value if it already exists.
    pub async fn add_or_update_node_label(
        client: &Client,
        node_name: &str,
        label_key: &str,
        label_value: &str,
    ) -> Result<(), anyhow::Error> {
        let nodes = Api::<Node>::all(client.clone());

        let patch_params = PatchParams::default();

        let patch = json!({
            "metadata": {
                "labels": {
                    label_key: label_value
                }
            }
        });

        nodes
            .patch(node_name, &patch_params, &Patch::Merge(patch))
            .await?;
        Ok(())
    }

    /// A convenience function to create a node by name.
    /// This does nothing if the node already exists.
    async fn create_node(client: Client, node_name: &str) -> Result<(), anyhow::Error> {
        let nodes: Api<Node> = Api::all(client.clone());

        let node = Node {
            metadata: ObjectMeta {
                name: Some(node_name.to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        // Check if it already exists
        let node_list = nodes.list(&Default::default()).await.unwrap();
        if node_list
            .items
            .iter()
            .any(|n| n.metadata.name == Some(node_name.to_string()))
        {
            return Ok(());
        }
        let node = nodes.create(&PostParams::default(), &node).await?;
        assert_eq!(node.metadata.labels, None);
        wait_for_node(client.clone(), node_name, true).await;
        // Check if the node was added
        let node_list = nodes.list(&Default::default()).await.unwrap();
        assert!(node_list
            .items
            .iter()
            .any(|n| n.metadata.name == Some(node_name.to_string())));
        Ok(())
    }

    /// A convenience function to delete a node by name.
    async fn delete_node(client: Client, node_name: &str) -> Result<(), anyhow::Error> {
        let nodes: Api<Node> = Api::all(client.clone());
        nodes.delete(node_name, &Default::default()).await?;
        wait_for_node(client.clone(), node_name, false).await;
        assert!(!nodes
            .list(&Default::default())
            .await
            .unwrap()
            .items
            .iter()
            .any(|n| n.metadata.name == Some(node_name.to_string())));
        Ok(())
    }

    /// Set the value of the label on the node to a random string.
    async fn set_random_label(
        client: Client,
        node_name: &str,
        key: &str,
    ) -> Result<String, anyhow::Error> {
        let random_length: usize = rng().random_range(1..=63);
        let value: String = rng()
            .sample_iter(&Alphanumeric)
            .take(random_length)
            .map(char::from)
            .collect();
        let mut new_labels = BTreeMap::new();
        new_labels.insert(key.to_string(), value.clone());
        set_node_labels(&client, node_name, &new_labels)
            .await
            .unwrap();
        wait_for_label_value(client.clone(), node_name, key, Some(&value)).await;
        Ok(value)
    }

    /// Poll until a node does or does not exist
    async fn wait_for_node(client: Client, node_name: &str, should_exist: bool) {
        let nodes: Api<Node> = Api::all(client);
        let interval = std::time::Duration::from_millis(200);
        let timeout = std::time::Duration::from_secs(10);
        let start = std::time::Instant::now();
        loop {
            let exists = nodes.get(node_name).await.is_ok();
            if exists == should_exist {
                return;
            }
            if start.elapsed() > timeout {
                panic!(
                    "Timeout waiting for node {} (should_exist: {})",
                    node_name, should_exist
                );
            }
            tokio::time::sleep(interval).await;
        }
    }

    /// Poll until a node has a specific label value
    async fn wait_for_label_value(
        client: Client,
        node_name: &str,
        key: &str,
        value: Option<&String>,
    ) {
        let nodes: Api<Node> = Api::all(client.clone());
        let interval = std::time::Duration::from_millis(500);
        let timeout = std::time::Duration::from_secs(10);
        let start = std::time::Instant::now();
        loop {
            if let Ok(node) = nodes.get(node_name).await {
                let current_value = node.metadata.labels.as_ref().and_then(|l| l.get(key));
                if current_value == value {
                    return;
                }
            }
            if start.elapsed() > timeout {
                let node = nodes.get(node_name).await.ok();
                panic!(
                    "Timeout waiting for node {} label {} to have value {:?}. Current: {:?}",
                    node_name,
                    key,
                    value,
                    node.and_then(|n| n.metadata.labels)
                );
            }
            tokio::time::sleep(interval).await;
        }
    }

    /// Poll until a node has no labels
    async fn wait_for_no_labels(client: Client, node_name: &str) {
        let nodes: Api<Node> = Api::all(client);
        let interval = std::time::Duration::from_millis(500); // Poll interval
        let timeout = std::time::Duration::from_secs(20); // Timeout for controller action
        let start = std::time::Instant::now();
        loop {
            match nodes.get(node_name).await {
                Ok(node) => {
                    if node.metadata.labels.as_ref().is_none() {
                        return;
                    }
                }
                Err(e) => {
                    panic!("Asked to wait for no labels, but node not found: {}", e);
                }
            }
            if start.elapsed() > timeout {
                let node = nodes.get(node_name).await.ok(); // Get final state for logging
                panic!(
                    "Timeout waiting for node '{}' to have no labels after {}s. Final labels: {:?}",
                    node_name,
                    timeout.as_secs(),
                    node.map(|node| node.metadata.labels.clone())
                );
            }
            tokio::time::sleep(interval).await;
        }
    }

    /// Generate a random node name of a given length.
    fn random_node_name(length: usize) -> String {
        let name: String = rng()
            .sample_iter(&Alphanumeric)
            .take(length)
            .map(char::from)
            .map(|c| c.to_ascii_lowercase())
            .collect();
        name
    }

    /// Generate a random node name of a random length between min and max length.
    fn random_node_name_random_length() -> String {
        let random_length: usize = rng().random_range(1..=253);
        random_node_name(random_length)
    }

    /// Test the following scenario:
    /// 1. Create a node
    /// 2. Add a label to the node
    /// 3. Delete the node, add the node back to the cluster and assert that the label is restored
    #[tokio::test]
    async fn test_add_and_remove_node() {
        let client = Client::try_default().await.unwrap();

        //
        // 1. Create a node.
        //
        let test_node_name = random_node_name(253);
        create_node(client.clone(), &test_node_name).await.unwrap();

        //
        // 2. Add a label to the node
        //
        let node_label_key = "label.to.persist.com/test_slash";
        let node_label_value = set_random_label(client.clone(), &test_node_name, node_label_key)
            .await
            .unwrap();

        //
        // 3. Delete the node, add the node back to the cluster, and assert that the label is restored
        //
        delete_node(client.clone(), &test_node_name).await.unwrap();
        create_node(client.clone(), &test_node_name).await.unwrap();
        wait_for_label_value(
            client.clone(),
            &test_node_name,
            node_label_key,
            Some(&node_label_value),
        )
        .await;
    }

    /// Test the following scenario:
    /// 1. Create a node
    /// 2. Add a label to the node
    /// 3. Delete the node and assert that the label is stored
    /// 4. Add the node back to the cluster with a different label already set. Assert that the new
    ///    label is not overwritten.
    /// This is testing the edge case where labels are set on a node that is brought back to the
    /// cluster before the processor has time to restore labels.
    #[tokio::test]
    async fn test_overwriting_labels() {
        let client = Client::try_default().await.unwrap();

        //
        // 1. Create a node.
        //
        let test_node_name = random_node_name_random_length();
        create_node(client.clone(), &test_node_name).await.unwrap();

        //
        // 2. Add labels to the node
        //
        let node_label_key = "label_to_persist";
        let node_label_value = set_random_label(client.clone(), &test_node_name, node_label_key)
            .await
            .unwrap();
        wait_for_label_value(
            client.clone(),
            &test_node_name,
            node_label_key,
            Some(&node_label_value.to_string()),
        )
        .await;
        let node_label_key2 = "label_to_persist2";
        let node_label_value2 = set_random_label(client.clone(), &test_node_name, node_label_key2)
            .await
            .unwrap();
        wait_for_label_value(
            client.clone(),
            &test_node_name,
            node_label_key2,
            Some(&node_label_value2.to_string()),
        )
        .await;

        //
        // 3. Delete the node so that the label is stored
        //
        delete_node(client.clone(), &test_node_name).await.unwrap();

        //
        // 4. Add the node back to the cluster with a different label already set.
        // Assert that the new label is not overwritten.
        //
        let new_label_value = "test_label_value";
        let new_key = "new_key";
        let new_key_value = "new_key_value";
        let nodes: Api<Node> = Api::all(client.clone());
        let node = Node {
            metadata: ObjectMeta {
                name: Some(test_node_name.to_string()),
                labels: Some(
                    vec![
                        (node_label_key.to_string(), new_label_value.to_string()),
                        (new_key.to_string(), new_key_value.to_string()),
                    ]
                    .into_iter()
                    .collect(),
                ),
                ..Default::default()
            },
            ..Default::default()
        };
        nodes.create(&PostParams::default(), &node).await.unwrap();
        wait_for_node(client.clone(), &test_node_name, true).await;
        // Check if the node was added
        let node_list = nodes.list(&Default::default()).await.unwrap();
        assert!(node_list
            .items
            .iter()
            .any(|n| n.metadata.name == Some(test_node_name.to_string())));

        // Assert that the node has the new label value
        wait_for_label_value(
            client.clone(),
            &test_node_name,
            node_label_key,
            Some(&new_label_value.to_string()),
        )
        .await;
        // Assert that the other label was unaffected
        wait_for_label_value(
            client.clone(),
            &test_node_name,
            node_label_key2,
            Some(&node_label_value2.to_string()),
        )
        .await;
        // Assert that the label created on the recreated node was unaffected
        wait_for_label_value(
            client.clone(),
            &test_node_name,
            new_key,
            Some(&new_key_value.to_string()),
        )
        .await;
    }

    /// 1. Create a node
    /// 2. Add a label to the node.
    /// 3. Delete the node so that the label is stored
    /// 4. Add the node back to the cluster and assert that the label is restored
    /// 5. Delete the label on the node
    /// 6. Cycle the node again and see that the label is not restored
    #[tokio::test]
    async fn test_deleting_labels() {
        // Start from a clean slate
        let client = Client::try_default().await.unwrap();

        //
        // 1. Create a node.
        //
        let test_node_name = random_node_name(1);
        create_node(client.clone(), &test_node_name).await.unwrap();

        //
        // 2. Add a label to the node
        //
        let node_label_key = "label_to_persist_deleting.12345";
        let node_label_value = set_random_label(client.clone(), &test_node_name, node_label_key)
            .await
            .unwrap();
        add_or_update_node_label(&client, &test_node_name, node_label_key, &node_label_value)
            .await
            .unwrap();

        //
        // 3. Delete the node so that labels are stored
        //
        delete_node(client.clone(), &test_node_name).await.unwrap();

        //
        // 4. Add the node back to the cluster and assert that the label is restored
        //
        create_node(client.clone(), &test_node_name).await.unwrap();
        wait_for_label_value(
            client.clone(),
            &test_node_name,
            node_label_key,
            Some(&node_label_value),
        )
        .await;

        //
        // 5. Delete the label on the node
        //
        delete_node_label(&client, &test_node_name, node_label_key)
            .await
            .unwrap();
        let nodes: Api<Node> = Api::all(client.clone());
        // The node should not have the key that was deleted
        let node_label_keys = nodes
            .get(&test_node_name)
            .await
            .unwrap()
            .metadata
            .labels
            .unwrap_or_default()
            .keys()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
        assert!(!node_label_keys.contains(&node_label_key.to_string()));

        //
        // 6. Cycle the node again and see that the label is not restored
        //
        delete_node(client.clone(), &test_node_name).await.unwrap();
        create_node(client.clone(), &test_node_name).await.unwrap();
        // The node should not have the key that was deleted
        let node_label_keys = nodes
            .get(&test_node_name)
            .await
            .unwrap()
            .metadata
            .labels
            .unwrap_or_default()
            .keys()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
        assert!(!node_label_keys.contains(&node_label_key.to_string()));
    }

    /// Test the scenario where a node starts with no labels, is deleted,
    /// and then recreated. It should still have no labels after recreation.
    #[tokio::test]
    async fn test_no_labels_cycle() {
        let client = Client::try_default().await.unwrap();
        let test_node_name = "node-no-labels-cycle-test"; // Unique name for this test

        // Ensure clean slate before starting
        let _ = delete_node(client.clone(), test_node_name).await;
        wait_for_node(client.clone(), test_node_name, false).await; // Wait for deletion confirmation

        // 1. Create a node. It should have no labels by default.
        create_node(client.clone(), test_node_name).await.unwrap();
        wait_for_node(client.clone(), test_node_name, true).await; // Wait for creation

        // Assertion 1: Verify it initially has no labels
        wait_for_no_labels(client.clone(), test_node_name).await;

        // 2. Delete the node. Controller's cleanup_node should run.
        //    It will create a ConfigMap storing no labels (only the flag).
        delete_node(client.clone(), test_node_name).await.unwrap();
        wait_for_node(client.clone(), test_node_name, false).await; // Wait for deletion

        // 3. Recreate the node. Controller's apply_node should run.
        //    It should read the ConfigMap, find no labels to restore, and do nothing to node labels.
        create_node(client.clone(), test_node_name).await.unwrap();
        wait_for_node(client.clone(), test_node_name, true).await; // Wait for recreation

        // 4. Wait for the controller to potentially reconcile and verify the node still has no labels.
        //    The wait ensures the controller has had a chance to act (or correctly do nothing).
        wait_for_no_labels(client.clone(), test_node_name).await;
    }
}
