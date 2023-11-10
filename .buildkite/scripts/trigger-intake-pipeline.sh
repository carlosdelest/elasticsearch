#!/bin/bash

set -euo pipefail

# We have permission issues when the user commits to the repo using an email address that is not connected to their Buildkite account
# This means that Buildkite can't match them up to a user in Buildkite, so they are a sort of anonymous user
# The build still triggers okay, but it's not able to trigger other builds, because the "user" doesn't have permission

# So, we're triggering builds using an API token and a known user in Buildkite

scripts_dir=$(dirname "$0")
source "$scripts_dir/utils/misc.sh"

INTAKE_PIPELINE_SLUG="elasticsearch-serverless-intake"
BUILDKITE_API_TOKEN=$(vault_with_retries read -field=token secret/ci/elastic-elasticsearch-serverless/buildkite-api-token)

json=$(cat <<EOF
{
  "commit": "${BUILDKITE_COMMIT}",
  "branch": "${BUILDKITE_BRANCH}",
  "author": {
    "name": "${BUILDKITE_BUILD_AUTHOR}",
    "email": "${BUILDKITE_BUILD_AUTHOR_EMAIL}"
  }
}
EOF
)

echo '--- Triggering build'

resp=$(curl -sX POST "https://api.buildkite.com/v2/organizations/elastic/pipelines/${INTAKE_PIPELINE_SLUG}/builds" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $BUILDKITE_API_TOKEN" \
  -d "$json")

echo "Response:"
echo "$resp"

url=$(echo "$resp" | jq -r .web_url)

echo "+++ Triggered build"
echo "Triggered build:"
printf '\033]1339;url='%s'\a\n' "$url" # Makes the url clickable in the Buildkite UI

cat <<EOF | buildkite-agent annotate --style info --context "triggered-build"
Triggered build:
[$url]($url)
EOF
