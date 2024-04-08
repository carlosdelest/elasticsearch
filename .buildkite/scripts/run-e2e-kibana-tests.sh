#!/bin/bash
set -e

scripts_dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
source $scripts_dir/utils/misc.sh

echo "--- Determining last succesful kibana e2e tests"
KIBANA_BRANCH='main'
INTAKE_PIPELINE_SLUG="kibana-elasticsearch-serverless-verify-and-promote"
BUILDKITE_API_TOKEN=$(vault_with_retries read -field=token secret/ci/elastic-elasticsearch-serverless/buildkite-api-token)
BUILD_JSON=$(curl -H "Authorization: Bearer ${BUILDKITE_API_TOKEN}" "https://api.buildkite.com/v2/organizations/elastic/pipelines/${INTAKE_PIPELINE_SLUG}/builds?branch=${KIBANA_BRANCH}&state=passed&per_page=100" | jq '. | map(. | select(.env.PUBLISH_MANIFEST? == "true")) | .[0] | {commit: .commit, url: .web_url}')
KIBANA_COMMIT=$(echo ${BUILD_JSON} | jq -r '.commit')
PROMOTED_BUILD_URL=$(echo ${BUILD_JSON} | jq -r '.url')

echo "Last succesful kibana e2e test build: ${PROMOTED_BUILD_URL}" | buildkite-agent annotate --style "info" --context "e2e-test-base"
echo "Kibana: $KIBANA_COMMIT / $KIBANA_BRANCH" | buildkite-agent annotate --style "info" --context "kibana-version"

echo "--- Trigger kibana e2e tests"
cat <<EOF | buildkite-agent pipeline upload
steps:
    - label: ":pipeline: Trigger Kibana E2E with current ES Serverless"
      trigger: kibana-elasticsearch-serverless-verify-and-promote
      depends_on: docker-publish
      build:
        commit: "${KIBANA_COMMIT}"
        branch: "${KIBANA_BRANCH}"
        env:
          ES_SERVERLESS_IMAGE: "${ES_SERVERLESS_IMAGE}"
          SKIP_CYPRESS: "1"
          FTR_EXTRA_ARGS: "--include-tag=esGate"
EOF
