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

API_KEY=$(vault_with_retries read -field api-key "$VAULT_PATH_API_KEY")

PROJECT_ID=${PROJECT_ID:-$(buildkite-agent meta-data get ess-project-id --default '')}
if [ -z "${PROJECT_ID}" ]; then
    if [ "${BUILDKITE_PIPELINE_SLUG}" == "elasticsearch-serverless-undeploy-qa" ]; then
        echo "--- Resolve project id from latest deployment build from build"
        DEPLOY_PIPELINE_SLUG="elasticsearch-serverless-deploy-qa"
        BUILDKITE_API_TOKEN=$(vault_with_retries read -field=token secret/ci/elastic-elasticsearch-serverless/buildkite-api-token)
        BUILD_JSON=$(curl -H "Authorization: Bearer ${BUILDKITE_API_TOKEN}" "https://api.buildkite.com/v2/organizations/elastic/pipelines/${DEPLOY_PIPELINE_SLUG}/builds?branch=${BUILDKITE_BRANCH}&state=passed" | jq '.[0]')
        DEPLOY_BUILD_URL=$(echo ${BUILD_JSON} | jq -r '.web_url')
        PROJECT_ID=$(echo ${BUILD_JSON} | jq -r '.meta_data."ess-project-id"')
    else
        echo "--- Resolve project id from deployID $DEPLOY_ID"
        ALL_PROJECTS=$(curl -H "Authorization: ApiKey $API_KEY" "${ENV_URL}/api/v1/serverless/projects/$PROJECT_TYPE")
        PROJECT_ID=$(echo $ALL_PROJECTS | jq -c --arg deploymentName "$DEPLOY_ID" '.items[] | select(.name == $deploymentName)' | jq -r '.id')
    fi
fi

echo "Deleting project $PROJECT_ID"
curl -XDELETE -H "Authorization: ApiKey $API_KEY" "${ENV_URL}/api/v1/serverless/projects/$PROJECT_TYPE/${PROJECT_ID}"
