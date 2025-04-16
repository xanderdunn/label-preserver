## Problem
Within a Kubernetes cluster, nodes are often added/deleted as they undergo maintenance with cloud providers. When this happens, metadata stored in the Kubernetes "Node" object is lost. This can be undesirable when using dedicated capacity, as you would like some data such as any Node labels to be kept across the node leaving/entering the cluster.

Write a service that will preserve Nodes’ labels if they are deleted from the cluster and re-apply them if they enter back into the cluster. This service itself should be stateless, but can use Kubernetes for any state storage.

## Assumptions
- If a node is added back to the cluster and it already has labels on it, we do a merge where labels with the same key are not overwritten. If a node is created with specific labels on it, we assume those labels are the latest. It's easy to flip this assumption and overwrite existing labels if desired.
- We store all of the labels for a single node in a single ConfigMap. This assumes all key:value label pairs for any one node are not more than 1MB in size.
- We serialize the label keys and values to JSON so we can handle arbitrary strings in the keys, includes slashes.

## Deploy and Run Tests
- Setup
    - Install Rust: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
    - [Install minikube](https://minikube.sigs.k8s.io/docs/start/)
    - [Install Docker](https://docs.docker.com/engine/install/)
- Build, Deploy Locally, and Test
    - `./test.sh`

## Dev Loop Setup
- `minikube start`
- `cargo run`
- `cargo test test_add_and_remove_node`
