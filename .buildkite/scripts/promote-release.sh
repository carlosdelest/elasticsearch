#!/bin/bash
set -e

if [ -z "${PROMOTED_COMMIT}" ]; then
  echo "--- Determining promotion commit"
  INTAKE_PIPELINE_SLUG="elasticsearch-serverless-intake"
  BUILDKITE_API_TOKEN=$(vault read -field=token secret/ci/elastic-elasticsearch-serverless/buildkite-api-token)
  BUILD_JSON=$(curl -H "Authorization: Bearer ${BUILDKITE_API_TOKEN}" "https://api.buildkite.com/v2/organizations/elastic/pipelines/${INTAKE_PIPELINE_SLUG}/builds?branch=main&state=passed" | jq '.[0] | {commit: .commit, url: .web_url}')
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
      env:
        GPCTL_CONFIG: https://raw.githubusercontent.com/elastic/serverless-gitops/main/gen/gpctl/elasticsearch/config.yaml
        GITOPS_ENV: qa
EOF
