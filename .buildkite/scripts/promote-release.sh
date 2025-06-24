#!/bin/bash
set -e

scripts_dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
source $scripts_dir/utils/misc.sh

GPCTL_CONFIG="${GPCTL_CONFIG:-https://raw.githubusercontent.com/elastic/serverless-gitops/main/gen/gpctl/elasticsearch/config.yaml}"

if [[ "${BUILDKITE_BRANCH}" != "main" ]] && [[ "${BUILDKITE_BRANCH}" != patch/* ]]; then
  echo "Invalid release branch '${BUILDKITE_BRANCH}. Valid branches are 'main' or prefixed with 'patch/'."
  exit 1
fi

if [[ -z "${PROMOTED_COMMIT}" ]]; then
  BUILDKITE_API_TOKEN=$(vault_with_retries read -field=token secret/ci/elastic-elasticsearch-serverless/buildkite-api-token)
  INTAKE_PIPELINE_SLUG="elasticsearch-serverless-intake"

  if [[ "${BUILDKITE_BRANCH}" != "main" ]] || [[ "${SKIP_ML_QA_CHECKS:-false}" == "true" ]]; then
    echo "--- Determining promotion commit by latest successful intake build"
    INTAKE_BUILD_JSON=$(curl -H "Authorization: Bearer ${BUILDKITE_API_TOKEN}" "https://api.buildkite.com/v2/organizations/elastic/pipelines/${INTAKE_PIPELINE_SLUG}/builds?branch=${BUILDKITE_BRANCH}&state=passed" | jq '. | map(select(.env.GITOPS_ENV == "dev")) | .[0] | {commit: .commit, url: .web_url}')
    PROMOTED_COMMIT=$(echo ${INTAKE_BUILD_JSON} | jq -r '.commit')
    PROMOTED_BUILD_URL=$(echo ${INTAKE_BUILD_JSON} | jq -r '.url')
    echo "Promoted intake build: ${PROMOTED_BUILD_URL}" | buildkite-agent annotate --priority 9 --style "info" --context "promoted-build-url"
  else
    echo "--- Determining last successful ML QA build and related intake build"
    ML_QA_PIPELINE_SLUG="appex-qa-serverless-ml-scenarios"
    ML_QA_BUILD_JSON=$(curl -H "Authorization: Bearer ${BUILDKITE_API_TOKEN}" "https://api.buildkite.com/v2/organizations/elastic/pipelines/${ML_QA_PIPELINE_SLUG}/builds?branch=${BUILDKITE_BRANCH}&state=passed" | jq '. | map(select(.env.ML_SERVERLESS_QUALITY_GATE == "true")) | .[0] | {commit: .commit, url: .web_url, es_hash: .meta_data."elasticsearch-serverless-commit-hash"}')
    PROMOTED_COMMIT=$(echo ${ML_QA_BUILD_JSON} | jq -r '.es_hash')
    ML_QA_BUILD_URL=$(echo ${ML_QA_BUILD_JSON} | jq -r '.url')
    ML_QA_CHECKED_INTAKE_BUILD_JSON=$(curl -H "Authorization: Bearer ${BUILDKITE_API_TOKEN}" "https://api.buildkite.com/v2/organizations/elastic/pipelines/${INTAKE_PIPELINE_SLUG}/builds?branch=${BUILDKITE_BRANCH}&state=passed&commit=${PROMOTED_COMMIT}" | jq '.[0] | {commit: .commit, url: .web_url}')
    ML_QA_CHECKED_INTAKE_BUILD_URL=$(echo ${ML_QA_CHECKED_INTAKE_BUILD_JSON} | jq -r '.url')
    cat << EOF | buildkite-agent annotate --context "promoted-build-url" --priority 9 --style "info"
Promoted intake build: ${ML_QA_CHECKED_INTAKE_BUILD_URL}
ML QA build: ${ML_QA_BUILD_URL}
EOF
  fi
  echo "Lock/Unlock qa / staging environnment https://argo-workflows.cd.internal.qa.elastic.cloud/login?redirect=https://argo-workflows.cd.internal.qa.elastic.cloud/workflow-templates/argo-events/gpctl-locking-management?sidePanel=submit" | buildkite-agent annotate --style "info" --context "lock-qa-staging"
  echo "Lock/Unlock canary / non canary prod environment https://argo-workflows.cd.internal.elastic.cloud/login?redirect=https://argo-workflows.cd.internal.elastic.cloud/workflow-templates/argo-events/gpctl-locking-management?sidePanel=submit" | buildkite-agent annotate --style "info" --context "lock-prod"
fi

if [ -z "${PREVIOUS_PROMOTED_COMMIT}" ]; then
  echo "--- Determining current prod version"
  SERVICE_VERSION_YAML_ENCODED=$(curl -H "Authorization: token ${GITHUB_TOKEN}" https://api.github.com/repos/elastic/serverless-gitops/contents/services/elasticsearch/versions.yaml | jq -r '.content')
  SERVICE_VERSION_YAML=$(echo "${SERVICE_VERSION_YAML_ENCODED}" | base64 -d)
  PREVIOUS_PROMOTED_COMMIT=$(echo "${SERVICE_VERSION_YAML}" | yq e '.services.elasticsearch.versions.production-noncanary-ds-1' -)
fi

echo "Promoting from commit '$PREVIOUS_PROMOTED_COMMIT' to commit '${PROMOTED_COMMIT}'"
buildkite-agent meta-data set "promoted-commit" "${PROMOTED_COMMIT}"
SHORT_COMMIT=$(echo "${PROMOTED_COMMIT}" | cut -c1-12)
DOCKER_IMAGE="docker.elastic.co/elasticsearch-ci/elasticsearch-serverless:git-${SHORT_COMMIT}"

echo "--- Trigger release build"
cat <<EOF | buildkite-agent pipeline upload
steps:
  - label: ":github: Check Promotion Blocker"
    key: "checkblocker"
    command: |
          .buildkite/scripts/run-gradle.sh checkPromotionBlocker
          buildkite-agent artifact upload "build/reports/blockers/serverless-promotion-blocker.json"
    env:
        USE_GITHUB_CREDENTIALS: "true"
        BLOCK_ON_ISSUES_UNTRIAGED: ${BLOCK_ON_ISSUES_UNTRIAGED}
        BLOCK_ON_ISSUES_BLOCKER: ${BLOCK_ON_ISSUES_BLOCKER}
    agents:
      provider: "gcp"
      machineType: "n1-standard-16"
      image: family/elasticsearch-ubuntu-2022
  - label: ":git: Validate patch branch has been merged"
    key: "validate-patch-merged"
    command: ".buildkite/scripts/validate-patch-merged.sh"
    env:
      BLOCK_ON_PATCH_BRANCH_NOT_MERGED: ${BLOCK_ON_PATCH_BRANCH_NOT_MERGED}
      PROMOTED_COMMIT: ${PROMOTED_COMMIT}
  - label: ":docker: Trigger cve-slo-status check"
    key: "cve-slo-status"
    trigger: cve-slo-status
    build:
      message: "Checking CVE SLO status for ${DOCKER_IMAGE}"
      env:
        CONTAINER: "${DOCKER_IMAGE}"
        USE_VM_AGENT: "true"
    soft_fail: true
  - label: ":argo: Trigger serverless Elasticsearch release"
    depends_on:
      - "checkblocker"
      - "validate-patch-merged"
      - "cve-slo-status"
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
