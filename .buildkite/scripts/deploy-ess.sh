#!/usr/bin/env bash

# ELASTICSEARCH CONFIDENTIAL
# __________________
#
#  Copyright Elasticsearch B.V. All rights reserved.
#
# NOTICE:  All information contained herein is, and remains
# the property of Elasticsearch B.V. and its suppliers, if any.
# The intellectual and technical concepts contained herein
# are proprietary to Elasticsearch B.V. and its suppliers and
# may be covered by U.S. and Foreign Patents, patents in
# process, and are protected by trade secret or copyright
# law.  Dissemination of this information or reproduction of
# this material is strictly forbidden unless prior written
# permission is obtained from Elasticsearch B.V.

set -euo pipefail

source "$BUILDKITE_DIR/scripts/utils/platform.sh"
source "$BUILDKITE_DIR/scripts/utils/misc.sh"

resolvePlatformEnvironment

echo '--- Create Project via project-api'

CREATE_RESULT=$(curl -k -H "Authorization: ApiKey $API_KEY" \
     -H "Content-Type: application/json" "https://$PAPI_PUBLIC_IP:8443/api/v1/serverless/projects/elasticsearch" \
     -XPOST -d "{
        \"name\": \"$DEPLOY_ID\",
        \"region_id\": \"local-k8s\",
        \"overrides\": {
            \"elasticsearch\": {
                \"docker_image\": \"$INTEG_TEST_IMAGE\"
            }
        }
     }")

echo "PROJECT API CREATE RESPONSE: $CREATE_RESULT"

PROJECT_ID=$(echo $CREATE_RESULT | jq -r '.id')
ESS_ROOT_USERNAME=$(echo $CREATE_RESULT | jq -r '.credentials.username')
ESS_ROOT_PASSWORD=$(echo $CREATE_RESULT | jq -r '.credentials.password')

# wait for the project namespace to be created

echo '--- Wait for ess pods be ready'

retry 5 30 "kubectl wait --for=condition=Ready pods --all -n project-$PROJECT_ID --timeout=240s"

echo "--- Testing ess access"

kubectl get svc ess-dev-proxy -n elastic-system -o json 
# aws does not expose ip but hostname 
LBS_HOST=$(kubectl get svc ess-dev-proxy -n elastic-system -o json | jq -r '.status.loadBalancer.ingress[0].hostname')
curl -k -H "X-Found-Cluster: $PROJECT_ID.es" -u $ESS_ROOT_USERNAME:$ESS_ROOT_PASSWORD https://$LBS_HOST

echo "Elasticsearch cluster available via https://$LBS_HOST" | buildkite-agent annotate --style "info" --context "ess-public-url"

ESS_API_KEY_RESPONSE=$(curl -k -H "Content-Type: application/json" -H "X-Found-Cluster: $PROJECT_ID.es" -u $ESS_ROOT_USERNAME:$ESS_ROOT_PASSWORD https://$LBS_HOST/_security/api_key -XPOST -d'
{
  "name": "elastic-test-user-api-key",
  "expiration": "2d",   
  "metadata": {
    "application": "ess-dev-test",
    "environment": {
       "level": 1,
       "trusted": true,
       "tags": ["dev", "ci"]
    }
  }
}')

ESS_API_KEY_ENCODED=$(echo $ESS_API_KEY_RESPONSE | jq -r '.encoded') 
ESS_API_KEY_ENCRYPTED=$(encrypt $ESS_API_KEY_ENCODED)

echo "$ESS_API_KEY_ENCRYPTED" | buildkite-agent annotate --style "info" --context "ess-api-key-encrypted"
echo "$PROJECT_ID" | buildkite-agent annotate --style "info" --context "project-id"

buildkite-agent meta-data set "ess-project-id" "$PROJECT_ID"
buildkite-agent meta-data set "ess-public-url" "https://$LBS_HOST"
buildkite-agent meta-data set "ess-api-key-encrypted" "$ESS_API_KEY_ENCRYPTED"
