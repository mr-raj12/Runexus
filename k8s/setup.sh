#!/bin/bash
set -e

echo "Creating kind cluster..."
kind create cluster --name workflow-cluster 2>/dev/null || echo "Cluster already exists"

echo "Applying manifests..."
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/rbac.yaml
kubectl apply -f k8s/runner-pod.yaml

echo "Waiting for StatefulSet rollout..."
kubectl rollout status statefulset/runner -n workflow-runner --timeout=120s

echo "Waiting for all runner pods to exist..."
until [ "$(kubectl get pods -l app=runner -n workflow-runner --no-headers 2>/dev/null | wc -l | tr -d ' ')" = "7" ]; do
  sleep 1
done

echo "Waiting for runner pods to be ready..."
kubectl wait --for=condition=Ready pod -l app=runner -n workflow-runner --timeout=120s

echo ""
echo "✅ Setup complete. 7 runner pods are live."
echo "Run: bun run dev"
