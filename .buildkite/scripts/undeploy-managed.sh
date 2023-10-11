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

ALL_PROJECTS=$(curl -H "Authorization: ApiKey $API_KEY" "${ENV_URL}/api/v1/serverless/projects/elasticsearch")
PROJECT_ID=$(echo $ALL_PROJECTS | jq -c --arg deploymentName "$DEPLOY_ID" '.items[] | select(.name == $deploymentName)' | jq -r '.id')

echo "Deleting project $PROJECT_ID"
curl -XDELETE -H "Authorization: ApiKey $API_KEY" "${ENV_URL}/api/v1/serverless/projects/elasticsearch/${PROJECT_ID}"
