#!/bin/bash

set -e

scripts_dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
source $scripts_dir/utils/misc.sh

# Get the list of dependent steps
echo "--- Determining rerun status"
previousBuild=$(($BUILDKITE_BUILD_NUMBER - 1))
BUILDKITE_API_TOKEN=$(vault_with_retries read -field=token secret/ci/elastic-elasticsearch-serverless/buildkite-api-token)

PREVIOUS_BUILD_JSON=$(curl -H "Authorization: Bearer ${BUILDKITE_API_TOKEN}" -q "https://api.buildkite.com/v2/organizations/elastic/pipelines/${BUILDKITE_PIPELINE_SLUG}/builds/$previousBuild")
THIS_BUILD_JSON=$(curl -H "Authorization: Bearer ${BUILDKITE_API_TOKEN}" -q "https://api.buildkite.com/v2/organizations/elastic/pipelines/${BUILDKITE_PIPELINE_SLUG}/builds/$BUILDKITE_BUILD_NUMBER")

# Extract the relevant information from the build json to identify a rerun
PREVIOUS_FOOTPRINT=$(echo ${PREVIOUS_BUILD_JSON} | jq -r '. | {env: .env, commit: .commit}')
THIS_FOOTPRINT=$(echo ${THIS_BUILD_JSON} | jq -r '. | {env: .env, commit: .commit}')

if [[ $(jq --argjson json1 "$PREVIOUS_FOOTPRINT" --argjson json2 "$THIS_FOOTPRINT" -n '$json1 == $json2')  == "true" ]]; then
  echo "Detected a quality gate rerun"
  buildkite-agent meta-data set "rerun" "true"
  buildkite-agent step update "label" "Rerun detected"
  buildkite-agent annotate "This is a detected rerun of quality gate run $(echo $PREVIOUS_BUILD_JSON | jq -r '.web_url')" --style "warning"
else
  diff <(echo "$PREVIOUS_FOOTPRINT" | jq .) <(echo "$THIS_FOOTPRINT" | jq .) || true
  echo "No rerun detected"
fi