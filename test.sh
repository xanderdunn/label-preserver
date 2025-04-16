#!/usr/bin/env bash

APP_NAME="node-label-preserver"

# Kill the pod on exit
trap "kubectl delete deployment ${APP_NAME} && kubectl get pods --no-headers -o custom-columns=":metadata.name" | grep ${APP_NAME} | xargs -I {} kubectl delete pod {} --grace-period=0 --force --wait=true" EXIT

set -e

# Start minikube Docker container if it's not running
if minikube status > /dev/null 2>&1; then
  echo "Minikube is already running."
else
  echo "Starting minikube..."
  minikube start
fi

eval $(minikube -p minikube docker-env)

# Build Docker image
docker build -t $APP_NAME .

kubectl apply -f deployment.yaml --force
kubectl apply -f rbac.yaml
kubectl get deployments
kubectl describe deployment $APP_NAME
kubectl describe pod
kubectl get pods -l app=$APP_NAME
# kubectl logs -f node-label-preserver
# Wait for the pod to be ready
sleep 3
cargo test
