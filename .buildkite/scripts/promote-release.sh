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
  echo "Lock qa / staging environnment https://argo-workflows.cd.internal.qa.elastic.cloud/login?redirect=https://argo-workflows.cd.internal.qa.elastic.cloud/workflow-templates/argo-events/gpctl-locking-management?sidePanel=submit" | buildkite-agent annotate --style "info" --context "lock-qa-staging"
  echo "Lock canary / non canary prod environment https://argo-workflows.cd.internal.elastic.cloud/login?redirect=https://argo-workflows.cd.internal.elastic.cloud/workflow-templates/argo-events/gpctl-locking-management?sidePanel=submit" | buildkite-agent annotate --style "info" --context "lock-prod"
fi

if [ -z "${PREVIOUS_PROMOTED_COMMIT}" ]; then
  echo "--- Determining current prod version"
  SERVICE_VERSION_YAML=$(curl -H "Authorization: Bearer ${GITHUB_TOKEN}" https://raw.githubusercontent.com/elastic/serverless-gitops/main/services/elasticsearch/versions.yaml)
  PREVIOUS_PROMOTED_COMMIT=$(echo "${SERVICE_VERSION_YAML}" | yq e '.services.elasticsearch.versions.production-noncanary-ds-1' -)
fi
echo "Promoting from commit '$PREVIOUS_PROMOTED_COMMIT' to commit '${PROMOTED_COMMIT}'"

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
  - label: ":github: Generate Report"
    command: |
          .buildkite/scripts/run-gradle.sh generatePromotionReport --previousGitHash=${PREVIOUS_PROMOTED_COMMIT} -Dcurrent.promoted.version=${PROMOTED_COMMIT}
          buildkite-agent artifact upload "build/reports/promotion/serverless-promotion-report.html"
          buildkite-agent artifact upload "build/reports/promotion/serverless-promotion-report.json"
          cat << EOF | buildkite-agent annotate --style "info" --context "promotion-report"
            ### Promotion Report for ${PROMOTED_COMMIT}
            <a href="artifact://build/reports/promotion/serverless-promotion-report.html">serverless-promotion-report.html</a>
          EOF
    env:
      USE_GITHUB_CREDENTIALS: "true"
    agents:
      provider: "gcp"
      machineType: "n1-standard-16"
      image: family/elasticsearch-ubuntu-2022
EOF
