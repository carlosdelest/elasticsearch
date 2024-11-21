#!/bin/bash

set -euo pipefail

# Check to see if the PR linking automation has set configuration on this build for the submodule commit
if [[ -z "${ELASTICSEARCH_SUBMODULE_COMMIT:-}" && "${BUILDKITE_PIPELINE_SLUG:-}" == "elasticsearch-serverless-pull-request" ]]; then
  API_RESPONSE=$(curl -s -H "Authorization: Bearer $BUILDKITE_API_TOKEN" "https://api.buildkite.com/v2/organizations/elastic/pipelines/elasticsearch-serverless-pull-request/builds/$BUILDKITE_BUILD_NUMBER/annotations")
  ANNOTATION=$(echo "$API_RESPONSE" | jq -r '.[] | select(.context == "data-stateful-link") | .body_html')

  if [[ -n "$ANNOTATION" && "$ANNOTATION" != "null" ]]; then
    JSON=$(echo "$ANNOTATION" | sed 's/^<details><summary>Automation Data<\/summary><pre>//' | sed 's/<\/pre><\/details>$//')

    COMMIT=$(echo "$JSON" | jq -r '.elasticsearchCommit')

    if [[ -n "$COMMIT" && "$COMMIT" != "null" ]]; then
      export ELASTICSEARCH_SUBMODULE_COMMIT="$COMMIT"
    fi
  fi
fi

# If an explicit commit for the submodule is provided do a checkout
if [ -n "${ELASTICSEARCH_SUBMODULE_COMMIT:-}" ]; then
  git -C elasticsearch fetch origin "${ELASTICSEARCH_SUBMODULE_COMMIT}"
  git -C elasticsearch checkout "${ELASTICSEARCH_SUBMODULE_COMMIT}"
fi
