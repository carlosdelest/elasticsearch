#!/bin/bash

set -euo pipefail

API_RESPONSE=$(curl -s -H "Authorization: Bearer $BUILDKITE_API_TOKEN" "https://api.buildkite.com/v2/organizations/elastic/pipelines/elasticsearch-serverless-pull-request/builds/$BUILDKITE_BUILD_NUMBER/annotations")
ANNOTATION=$(echo "$API_RESPONSE" | jq -r '.[] | select(.context == "data-stateful-link") | .body_html')

if [[ -z "$ANNOTATION" || "$ANNOTATION" == "null" ]]; then
  exit 0
fi

JSON=$(echo "$ANNOTATION" | sed 's/^<details><summary>Automation Data<\/summary><pre>//' | sed 's/<\/pre><\/details>$//')

REPO_OWNER=$(echo "$JSON" | jq -r '.ownerForSubmodule')
COMMIT=$(echo "$JSON" | jq -r '.elasticsearchCommit')

if [[ -z "$COMMIT" || "$COMMIT" == "null" ]]; then
  exit 0
fi

cd elasticsearch
git remote add fork "git@github.com:$REPO_OWNER/elasticsearch.git"
git fetch fork "$COMMIT"
git checkout "$COMMIT"
cd -
