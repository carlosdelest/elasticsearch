#!/bin/bash
set -e

source "$BUILDKITE_DIR/scripts/utils/eks.sh"

resolvePlatformEnvironment() {
  echo "--- Resolve platform environment"
  aws_auth
  aws_get_cluster_credentials
  API_KEY=$(kubectl -n elastic-system get secret iam-service-test-user-apikey --template="{{ .data.apikey | base64decode }}")
  kubectl get svc project-api-dev-proxy -n elastic-system
  JSONOUTPUT=$(kubectl get svc project-api-dev-proxy -n elastic-system -o json)
  # AWS does not have exposed an ip but hostname
  PAPI_PUBLIC_IP=$(kubectl get svc project-api-dev-proxy -n elastic-system -o json | jq -r '.status.loadBalancer.ingress[0].hostname')
  echo "Project API IP: $PAPI_PUBLIC_IP"
}