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

echo "--- Resolve data from deployment build"
DEPLOY_PIPELINE_SLUG="elasticsearch-serverless-deploy-qa"
BUILDKITE_API_TOKEN=$(vault_with_retries read -field=token secret/ci/elastic-elasticsearch-serverless/buildkite-api-token)
BUILD_JSON=$(curl -H "Authorization: Bearer ${BUILDKITE_API_TOKEN}" "https://api.buildkite.com/v2/organizations/elastic/pipelines/${DEPLOY_PIPELINE_SLUG}/builds?branch=${BUILDKITE_BRANCH}&state=passed" | jq '.[0]')
DEPLOY_BUILD_URL=$(echo ${BUILD_JSON} | jq -r '.web_url')
PROJECT_ID=$(echo ${BUILD_JSON} | jq -r '.meta_data."ess-project-id"')

ESS_ROOT_USERNAME=$(echo ${BUILD_JSON} | jq -r '.meta_data."ess-username"')
ESS_PASSWORD_ENCRYPTED=$(echo ${BUILD_JSON} | jq -r '.meta_data."ess-password-encrypted"')
ESS_ROOT_PASSWORD=$(decrypt "$ESS_PASSWORD_ENCRYPTED")

echo "Updating deployment from: ${DEPLOY_BUILD_URL}" | buildkite-agent annotate --style "info" --context "updated-build-url"

API_KEY=$(vault_with_retries read -field api-key "$VAULT_PATH_API_KEY")

DEPLOYMENT_NAME=$(curl -k -H "Authorization: ApiKey $API_KEY" \
     -H "Content-Type: application/json" "${ENV_URL}/api/v1/serverless/projects/$PROJECT_TYPE/$PROJECT_ID" \
     | jq -r '.name')

echo "--- Updating deployment $PROJECT_ID - $DEPLOYMENT_NAME"
UPDATE_RESULT=$(curl -k -H "Authorization: ApiKey $API_KEY" \
     -H "Content-Type: application/json" "${ENV_URL}/api/v1/serverless/projects/$PROJECT_TYPE/$PROJECT_ID" \
     -XPUT -d "{
        \"name\": \"$DEPLOYMENT_NAME\",
        \"overrides\": {
            \"elasticsearch\": {
                \"docker_image\": \"$IMAGE_OVERRIDE\"
            }
        }
     }")

echo $UPDATE_RESULT

# wait for the project being ready again

echo '--- Wait for ES being ready'

PROJ_INFO=$(curl -H "Authorization: ApiKey $API_KEY" "${ENV_URL}/api/v1/serverless/projects/$PROJECT_TYPE/${PROJECT_ID}")
ES_ENDPOINT=$(echo $PROJ_INFO | jq -r '.endpoints.elasticsearch')

# wait for a maximum of 20 minutes max
retry 30 40 checkEssAvailability

echo "Elasticsearch cluster available via $ES_ENDPOINT" | buildkite-agent annotate --style "info" --context "ess-public-url"