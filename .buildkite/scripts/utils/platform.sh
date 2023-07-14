#!/bin/bash
set -e

source "$BUILDKITE_DIR/scripts/utils/gke.sh"

resolvePlatformEnvironment() {
  gke_get_cluster_credentials $GCLOUD_SERVICE_ACCOUNT_VAULT_PATH $GCLOUD_PROJECT $GKE_CLUSTER_NAME $GCLOUD_REGION
  API_KEY=$(kubectl -n elastic-system get secret iam-service-test-user-apikey --template="{{ .data.apikey | base64decode }}")
  PAPI_PUBLIC_IP=$(kubectl get svc project-api-dev-proxy -n elastic-system -o json | jq -r '.status.loadBalancer.ingress[0].ip')
}