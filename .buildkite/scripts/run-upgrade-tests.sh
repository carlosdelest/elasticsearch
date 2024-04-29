#!/bin/bash
set -e

if [ "${GITOPS_ENV}" == "dev" ]; then
  # If we're testing against dev then test all existing environments
  RELEASE_TAGS=(current_dev current_qa current_staging current_production-canary current_production-noncanary)
  RELEASE_COMMITS=()

  # If this is a patch release branch we don't want to do upgrade tests against dev
  if [[ "${BUILDKITE_BRANCH}" == patch/* ]]; then
    RELEASE_TAGS=(current_qa current_staging current_production-canary current_production-noncanary)
  fi

  for tag in ${RELEASE_TAGS[@]}; do
    RELEASE_COMMITS+=($(git rev-list -1 $tag))
  done

  UNIQUE_COMMITS=($(for c in ${RELEASE_COMMITS[@]}; do
    echo "$c"
  done | uniq))
else
  TAG_NAME="current_${GITOPS_ENV}"
  RELEASE_TAGS=("${TAG_NAME}")
  RELEASE_COMMITS=("${TAG_NAME}")
  UNIQUE_COMMITS=("${TAG_NAME}")
fi

function environments_for() {
    local environments=()
    local commit="$1"

    for i in ${!RELEASE_COMMITS[@]}; do
      if [ "${RELEASE_COMMITS[$i]}" == "${commit}" ]; then
        environments+=(${RELEASE_TAGS[$i]})
      fi
    done

    local result=$(printf ",%s" "${environments[@]}")
    echo -n "${result:1}"
}
echo "--- Upload pipeline"
PIPELINE=$(cat <<EOF
steps:
  - group: ":arrow_up: Run upgrade tests"
    steps:

EOF
)

for commit in ${UNIQUE_COMMITS[@]}; do
  PIPELINE+=$(cat <<EOF

      - label: ":java: Run upgrade tests for '$(environments_for $commit)'"
        command: ".buildkite/scripts/run-gradle.sh bwcTest -Dtests.bwc.tag=${commit}"
        agents:
          provider: "gcp"
          machineType: "n1-standard-16"
          image: family/elasticsearch-ubuntu-2022
        notify:
          - github_commit_status:
              context: "elasticsearch-serverless/test upgrade from $(environments_for $commit)"
EOF
)
done

echo "${PIPELINE}" | buildkite-agent pipeline upload

