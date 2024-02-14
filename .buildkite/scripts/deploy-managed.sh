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
source "$BUILDKITE_DIR/scripts/utils/misc.sh"

echo "--- Create Project via project-api in $TEST_ENV"

API_KEY=$(vault_with_retries read -field api-key "$VAULT_PATH_API_KEY")

# Create es override if we have declared one. empty IMAGE_OVERRIDE env means using default with no override
if [[ -z "${IMAGE_OVERRIDE}" ]]; then
  echo "No IMAGE_OVERRIDE declared"
OVERRIDE_CONFIG=""
else
OVERRIDE_CONFIG=",
    \"overrides\": {
        \"elasticsearch\": {
            \"docker_image\": \"$IMAGE_OVERRIDE\"
        }
    }"
fi

CREATE_RESULT=$(curl -H "Authorization: ApiKey $API_KEY" \
     -H "Content-Type: application/json" "${ENV_URL}/api/v1/serverless/projects/elasticsearch" \
     -XPOST -d "{
        \"name\": \"$DEPLOY_ID\",
        \"region_id\": \"$AWS_REGION\" $OVERRIDE_CONFIG
     }")

echo "PROJECT API CREATE RESPONSE: $CREATE_RESULT"

PROJECT_ID=$(echo $CREATE_RESULT | jq -e -r '.id')

# resolve credentials from reset credentials call
echo '--- Creating project'

resetCredentials() {
    CRED_RESULT=$(curl -H "Authorization: ApiKey $API_KEY" \
      -H "Content-Type: application/json" \
      "${ENV_URL}/api/v1/serverless/projects/elasticsearch/$PROJECT_ID/_reset-internal-credentials" \
      -XPOST)

    CREDCHECK=$((echo $CRED_RESULT | jq -e -r '.username') &> 0 && echo "true" || echo "false")
    if [ $CREDCHECK == "false" ]; then
        echo "Resetting credentials failed. Response: $CRED_RESULT"
    fi

    ESS_ROOT_USERNAME=$(echo $CRED_RESULT | jq -e -r '.username')
    ESS_ROOT_PASSWORD=$(echo $CRED_RESULT | jq -e -r '.password')
}

echo '--- Resetting credentials'
retry 5 5 resetCredentials

# wait for the project namespace to be created

echo '--- Wait for ES being ready'

PROJ_INFO=$(curl -H "Authorization: ApiKey $API_KEY" "${ENV_URL}/api/v1/serverless/projects/elasticsearch/${PROJECT_ID}")

ES_ENDPOINT=$(echo $PROJ_INFO | jq -r '.endpoints.elasticsearch')

# wait for a maximum of 20 minutes
retry 30 40 checkEssAvailability

echo "Elasticsearch cluster available via $ES_ENDPOINT" | buildkite-agent annotate --style "info" --context "ess-public-url"

#echo "--- Creating Elasticsearch API key"
#ESS_API_KEY_RESPONSE=$(curl -H "Content-Type: application/json" -u $ESS_ROOT_USERNAME:$ESS_ROOT_PASSWORD $ES_ENDPOINT/_security/api_key -XPOST -d"
#{
#  \"name\": \"elastic-test-user-api-key\",
#  \"expiration\": \"2d\",
#  \"metadata\": {
#    \"application\": \"ess-e2e-test\",
#    \"environment\": {
#       \"level\": 1,
#       \"trusted\": true,
#       \"tags\": [\"e2e\", \"$TEST_ENV\", \"ci\"]
#    }
#  }
#}")

#ESS_API_KEY_ENCODED=$(echo $ESS_API_KEY_RESPONSE | jq -e -r '.encoded')
#ESS_API_KEY_ENCRYPTED=$(encrypt $ESS_API_KEY_ENCODED)
ESS_ROOT_PASSWORD_ENCRYPTED=$(encrypt $ESS_ROOT_PASSWORD)

# expose this via buildkite annotations to make it accessible to other es engineers
echo "PROJECT_ID: ${PROJECT_ID}" | buildkite-agent annotate --style "info" --context "project-id"
echo "ESS_ROOT_PASSWORD_ENCRYPTED: ${ESS_ROOT_PASSWORD_ENCRYPTED}" | buildkite-agent annotate --style "info" --context "ess-password-encrypted"

buildkite-agent meta-data set "ess-project-id" "$PROJECT_ID"
buildkite-agent meta-data set "ess-public-url" "$ES_ENDPOINT"
#buildkite-agent meta-data set "ess-api-key-encrypted" "$ESS_API_KEY_ENCRYPTED"
buildkite-agent meta-data set "ess-username" "$ESS_ROOT_USERNAME"
buildkite-agent meta-data set "ess-password-encrypted" "$ESS_ROOT_PASSWORD_ENCRYPTED"
