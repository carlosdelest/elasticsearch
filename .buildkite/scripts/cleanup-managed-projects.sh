#!/usr/bin/env bash

#
# ELASTICSEARCH CONFIDENTIAL
# __________________
#
# Copyright Elasticsearch B.V. All rights reserved.
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
#

set -eo pipefail
source "$BUILDKITE_DIR/scripts/utils/misc.sh"

API_KEY=$(vault_with_retries read -field api-key "$VAULT_PATH_API_KEY")

# Grab any existing projects more than an hour old
read -ra OLD_PROJECTS <<< $(curl -k -H "Authorization: ApiKey $API_KEY" \
                            -H "Content-Type: application/json" \
                            ${ENV_URL}/api/v1/serverless/projects/${PROJECT_TYPE} \
                            | jq -r '.items | map(select(.metadata.created_at | sub(".[0-9]+Z$"; "Z") | fromdate < now - (60 * 60))) | map(.id) | join(" ")')

if [[ -n "${OLD_PROJECTS}" ]]; then
  for PROJECT_ID in "${OLD_PROJECTS[@]}"; do
    echo "Deleting project $PROJECT_ID"
    curl -XDELETE -H "Authorization: ApiKey $API_KEY" "${ENV_URL}/api/v1/serverless/projects/$PROJECT_TYPE/${PROJECT_ID}"
  done
fi
