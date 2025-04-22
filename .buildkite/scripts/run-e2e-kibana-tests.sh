#!/bin/bash
set -e

scripts_dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
source $scripts_dir/utils/misc.sh
echo "--- Determining last succesful kibana e2e tests"
KIBANA_BRANCH='main'
INTAKE_PIPELINE_SLUG="kibana-elasticsearch-serverless-verify-and-promote"

# Don't merge this back to main
# BUILDKITE_API_TOKEN=$(vault_with_retries read -field=token secret/ci/elastic-elasticsearch-serverless/buildkite-api-token)
# BUILD_JSON=$(curl -H "Authorization: Bearer ${BUILDKITE_API_TOKEN}" "https://api.buildkite.com/v2/organizations/elastic/pipelines/${INTAKE_PIPELINE_SLUG}/builds?branch=${KIBANA_BRANCH}&state=passed&per_page=100" | jq '. | map(. | select(.env.PUBLISH_DOCKER_TAG? == "true")) | .[0] | {id: .id, commit: .commit, url: .web_url}')

KIBANA_BUILD_ID="01965ef5-4ae2-4c7b-9150-a9a3da8e8aab"
KIBANA_COMMIT="c84380fe0137da7b83e83c92c1d6895a03026bcb"

PROMOTED_BUILD_URL="https://buildkite.com/elastic/kibana-elasticsearch-serverless-verify-and-promote/builds/3401"

echo "Last succesful kibana e2e test build: ${PROMOTED_BUILD_URL}" | buildkite-agent annotate --style "info" --context "e2e-test-base"
echo "Kibana: $KIBANA_COMMIT / $KIBANA_BRANCH" | buildkite-agent annotate --style "info" --context "kibana-version"

echo "--- Trigger kibana e2e tests"
cat <<EOF | buildkite-agent pipeline upload
steps:
    - label: ":rocket: Run Kibana E2E with current ES Serverless"
      trigger: kibana-elasticsearch-serverless-verify-and-promote
      depends_on: "${DOCKER_BUILD_STEP:-docker-publish}"
      build:
        commit: "${KIBANA_COMMIT}"
        branch: "${KIBANA_BRANCH}"
        env:
          ES_SERVERLESS_IMAGE: "${ES_SERVERLESS_IMAGE}"
          KIBANA_BUILD_ID: "${KIBANA_BUILD_ID}"
          SKIP_CYPRESS: "1"
          FTR_EXTRA_ARGS: "--include-tag=esGate"
EOF
