#!/bin/bash
set -exuo pipefail

echo "--- Tag git commit"
echo "Tagging commit ${BUILDKITE_COMMIT}"
git config user.name "elasticsearchmachine"
git config user.email "infra-root+elasticsearchmachine@elastic.co"
git tag -f current_dev ${BUILDKITE_COMMIT}
git tag -f current_qa ${BUILDKITE_COMMIT}
git push -f origin current_dev current_qa

echo "--- Tag and push 'latest' image manifest"
export GIT_ABBREV_COMMIT=git-${BUILDKITE_COMMIT:0:12}
export DOCKER_IMAGE=docker.elastic.co/elasticsearch-ci/elasticsearch-serverless
export X86_IMAGE_TAG=${DOCKER_IMAGE}:${GIT_ABBREV_COMMIT}-x86_64
export ARM_IMAGE_TAG=${DOCKER_IMAGE}:${GIT_ABBREV_COMMIT}-aarch64
docker manifest create ${DOCKER_IMAGE}:latest --amend ${X86_IMAGE_TAG} --amend ${ARM_IMAGE_TAG}
docker manifest push ${DOCKER_IMAGE}:latest
