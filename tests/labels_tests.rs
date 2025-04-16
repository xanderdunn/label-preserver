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

    /// A convenience function for asserting that the node's label value is correct.
    async fn assert_node_label_has_value(
        client: Client,
        node_name: &str,
        key: &str,
        value: Option<&String>,
    ) {
        let nodes: Api<Node> = Api::all(client.clone());
        let node = nodes.get(node_name).await.unwrap();
        assert_eq!(
            node.metadata.labels.as_ref().unwrap().get(key),
            value,
            "Node has {} value {:?} but expected {:?}",
            key,
            node.metadata.labels.as_ref().unwrap().get(key),
            value
        );
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
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
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
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
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
        assert_node_label_has_value(client.clone(), node_name, key, Some(&value)).await;
        Ok(value)
    }

    fn random_node_name(length: usize) -> String {
        let name: String = rng()
            .sample_iter(&Alphanumeric)
            .take(length)
            .map(char::from)
            .map(|c| c.to_ascii_lowercase())
            .collect();
        name
    }

    fn random_node_name_random_length() -> String {
        let random_length: usize = rng().random_range(1..=253);
        random_node_name(random_length)
    }

    #[tokio::test]
    /// Test the following scenario:
    /// 1. Create a node
    /// 2. Add a label to the node
    /// 3. Delete the node, add the node back to the cluster and assert that the label is restored
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
        tokio::time::sleep(std::time::Duration::from_millis(3000)).await;
        assert_node_label_has_value(
            client.clone(),
            &test_node_name,
            node_label_key,
            Some(&node_label_value),
        )
        .await;
    }

    #[tokio::test]
    /// Test the following scenario:
    /// 1. Create a node
    /// 2. Add a label to the node
    /// 3. Delete the node and assert that the label is stored
    /// 4. Add the node back to the cluster with a different label already set. Assert that the new
    ///    label is not overwritten.
    /// This is testing the edge case where labels are set on a node that is brought back to the
    /// cluster before the processor has time to restore labels.
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
        assert_node_label_has_value(
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
        assert_node_label_has_value(
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
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

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
        // Check if the node was added
        let node_list = nodes.list(&Default::default()).await.unwrap();
        assert!(node_list
            .items
            .iter()
            .any(|n| n.metadata.name == Some(test_node_name.to_string())));
        tokio::time::sleep(std::time::Duration::from_millis(3000)).await;

        // Assert that the node has the new label value
        assert_node_label_has_value(
            client.clone(),
            &test_node_name,
            node_label_key,
            Some(&new_label_value.to_string()),
        )
        .await;
        // Assert that the other label was unaffected
        assert_node_label_has_value(
            client.clone(),
            &test_node_name,
            node_label_key2,
            Some(&node_label_value2.to_string()),
        )
        .await;
        // Assert that the label created on the recreated node was unaffected
        assert_node_label_has_value(
            client.clone(),
            &test_node_name,
            new_key,
            Some(&new_key_value.to_string()),
        )
        .await;
    }

    #[tokio::test]
    /// 1. Create a node
    /// 2. Add a label to the node.
    /// 3. Delete the node so that the label is stored
    /// 4. Add the node back to the cluster and assert that the label is restored
    /// 5. Delete the label on the node
    /// 6. Cycle the node again and see that the label is not restored
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
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        //
        // 4. Add the node back to the cluster and assert that the label is restored
        //
        create_node(client.clone(), &test_node_name).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
        assert_node_label_has_value(
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
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        create_node(client.clone(), &test_node_name).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
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
}
