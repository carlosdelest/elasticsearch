#!/bin/bash
set -e

scripts_dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )

echo "--- Determining pull requests label"
if [[ "$BUILDKITE_PIPELINE_SLUG" == "elasticsearch-serverless-es-pr-check" ]]; then
    PR_REPO="elastic/elasticsearch"
    PR=${ELASTICSEARCH_PR_NUMBER}
    export DOCKER_BUILD_STEP="pr-docker-publish"
elif [[ "$BUILDKITE_PIPELINE_SLUG" == "elasticsearch-serverless-pull-request" ]]; then
    PR_REPO="elastic/elasticsearch-serverless"
    PR=${BUILDKITE_PULL_REQUEST}
else 
    echo "Cannot determine originating pull request"
    exit 1
fi  

label_names=($(curl -H "Authorization: token ${GITHUB_TOKEN}" https://api.github.com/repos/${PR_REPO}/pulls/$PR | jq -r '.labels[] | .name'))
kibana_test_label="test-kibana"

echo ${label_names[*]}
# Check if the label is in the array using pattern matching
if [[ " ${label_names[*]} " =~ " $kibana_test_label " ]]; then
    echo "Label '$kibana_test_label' found. Triggerering Kibana E2E tests"
    $scripts_dir/run-e2e-kibana-tests.sh
fi