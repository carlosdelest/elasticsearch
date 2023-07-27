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

retry 20 5 "kubectl wait --for=condition=Ready pods --all -n project-$PROJECT_ID --timeout=240s"

echo "--- Testing ess access"
ES_USERNAME=$(vault read -field username secret/ci/elastic-elasticsearch-serverless/gcloud-integtest-dev-ess-credentials)
ES_PASSWORD=$(vault read -field password secret/ci/elastic-elasticsearch-serverless/gcloud-integtest-dev-ess-credentials)
ESS_PUBLIC_IP=$(kubectl get svc ess-dev-proxy -n elastic-system -o json | jq -r '.status.loadBalancer.ingress[0].ip')

curl -k -u $ESS_ROOT_USERNAME:$ESS_ROOT_PASSWORD https://$PROJECT_ID.es.$ESS_PUBLIC_IP.ip.es.io

echo "Elasticsearch cluster available at https://$PROJECT_ID.es.$ESS_PUBLIC_IP.ip.es.io" | buildkite-agent annotate --style "info" --context "ess-public-url"

curl -k -H "Content-Type: application/json" "https://$PROJECT_ID.es.$ESS_PUBLIC_IP.ip.es.io/_security/user/$ES_USERNAME" -u $ESS_ROOT_USERNAME:$ESS_ROOT_PASSWORD \
-XPOST -d "{
  \"password\" : \"$ES_PASSWORD\",
  \"roles\" : [ \"superuser\"],
  \"full_name\" : \"Ess E2e Test user\",
  \"email\" : \"es-delivery@elastic.co\"
}"

curl -k -u $ES_USERNAME:$ES_PASSWORD https://$PROJECT_ID.es.$ESS_PUBLIC_IP.ip.es.io

buildkite-agent meta-data set "ess-public-url" "https://$PROJECT_ID.es.$ESS_PUBLIC_IP.ip.es.io"
