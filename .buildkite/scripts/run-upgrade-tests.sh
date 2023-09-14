#!/bin/bash
set -e

if [ "${GITOPS_ENV}" == "dev" ]; then
  # If we're testing against dev then test all existing environments
  RELEASE_TAGS=(current_dev current_qa current_staging current_production)
  RELEASE_COMMITS=()

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

    echo -n ${environments[@]}
}
echo "--- Upload pipeline"
PIPELINE=$(cat <<EOF
steps:
  - group: ":arrow_up: Run upgrade tests"
    if: build.env('GITOPS_ENV') == 'dev'
    steps:

EOF
)

for commit in ${UNIQUE_COMMITS[@]}; do
  PIPELINE+=$(cat <<EOF

      - label: ":java: Run upgrade tests for '$(environments_for $commit)'"
        command: ".buildkite/scripts/run-gradle.sh :qa:rolling-upgrade:check -Dtests.bwc.tag=${commit}"
        agents:
          provider: "gcp"
          machineType: "n1-standard-16"
          image: family/elasticsearch-ubuntu-2022
EOF
)
done

echo "${PIPELINE}" | buildkite-agent pipeline upload

