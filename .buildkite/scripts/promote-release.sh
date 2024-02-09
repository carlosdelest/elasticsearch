#!/bin/bash
set -e

scripts_dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
source $scripts_dir/utils/misc.sh

GPCTL_CONFIG="${GPCTL_CONFIG:-https://raw.githubusercontent.com/elastic/serverless-gitops/main/gen/gpctl/elasticsearch/config.yaml}"

if [[ "${BUILDKITE_BRANCH}" != "main" ]] && [[ "${BUILDKITE_BRANCH}" != patch/* ]]; then
  echo "Invalid release branch '${BUILDKITE_BRANCH}. Valid branches are 'main' or prefixed with 'patch/'."
  exit 1
fi

if [ -z "${PROMOTED_COMMIT}" ]; then
  echo "--- Determining promotion commit"
  INTAKE_PIPELINE_SLUG="elasticsearch-serverless-intake"
  BUILDKITE_API_TOKEN=$(vault_with_retries read -field=token secret/ci/elastic-elasticsearch-serverless/buildkite-api-token)
  BUILD_JSON=$(curl -H "Authorization: Bearer ${BUILDKITE_API_TOKEN}" "https://api.buildkite.com/v2/organizations/elastic/pipelines/${INTAKE_PIPELINE_SLUG}/builds?branch=${BUILDKITE_BRANCH}&state=passed" | jq '. | map(select(.env.GITOPS_ENV == "dev")) | .[0] | {commit: .commit, url: .web_url}')
  echo $BUILD_JSON
  PROMOTED_COMMIT=$(echo ${BUILD_JSON} | jq -r '.commit')
  PROMOTED_BUILD_URL=$(echo ${BUILD_JSON} | jq -r '.url')

  echo "Promoted build: ${PROMOTED_BUILD_URL}" | buildkite-agent annotate --style "info" --context "promoted-build-url"
fi

echo "Promoting commit '${PROMOTED_COMMIT}'"

echo "--- Trigger release build"
cat <<EOF | buildkite-agent pipeline upload
steps:
  - label: ":argo: Trigger serverless Elasticsearch release"
    trigger: elasticsearch-serverless-intake
    build:
      commit: "${PROMOTED_COMMIT}"
      branch: "${BUILDKITE_BRANCH}"
      env:
        GPCTL_CONFIG: ${GPCTL_CONFIG}
        GITOPS_ENV: qa
EOF
