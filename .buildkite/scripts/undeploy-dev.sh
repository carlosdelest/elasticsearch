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

resolvePlatformEnvironment

ALL_PROJECTS=$(curl -k -H 'Host: project-api' -H "Authorization: ApiKey $API_KEY" https://$PAPI_PUBLIC_IP:8443/api/v1/serverless/projects/elasticsearch)
PROJECT_ID=$(echo $ALL_PROJECTS | jq -c --arg deploymentName "$DEPLOY_ID" '.items[] | select(.name == $deploymentName)' | jq -r '.id')

echo "Deleting project $PROJECT_ID"
curl -k -H 'Host: project-api' \
     -H "Authorization: ApiKey $API_KEY" \
     https://$PAPI_PUBLIC_IP:8443/api/v1/serverless/projects/elasticsearch/$PROJECT_ID \
     -XDELETE
