#!/bin/bash
set -exuo pipefail

scripts_dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )

source $scripts_dir/utils/docker.sh

echo "--- Tag git commit"
echo "Tagging commit ${BUILDKITE_COMMIT}"
git config user.name "elasticsearchmachine"
git config user.email "infra-root+elasticsearchmachine@elastic.co"
git tag -f current_dev ${BUILDKITE_COMMIT}
git tag -f current_qa ${BUILDKITE_COMMIT}
git push -f origin current_dev current_qa

echo "--- Tag and push 'latest' image manifest"
GIT_ABBREV_COMMIT=${ ${DEPLOY_ID} : "git-${BUILDKITE_COMMIT:0:12}" }
DOCKER_IMAGE=docker.elastic.co/elasticsearch-ci/elasticsearch-serverless
X86_IMAGE_TAG=${DOCKER_IMAGE}:${GIT_ABBREV_COMMIT}-x86_64
ARM_IMAGE_TAG=${DOCKER_IMAGE}:${GIT_ABBREV_COMMIT}-aarch64
push_docker_manifest ${DOCKER_IMAGE}:latest ${X86_IMAGE_TAG} ${ARM_IMAGE_TAG}
