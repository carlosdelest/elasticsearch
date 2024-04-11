#!/bin/bash
set -euo pipefail

scripts_dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )

source $scripts_dir/utils/docker.sh

echo "--- Tag git commit"
TAG_NAME=current_${GITOPS_ENV}
echo "Tagging commit ${BUILDKITE_COMMIT}"
git config user.name "elasticsearchmachine"
git config user.email "infra-root+elasticsearchmachine@elastic.co"
git tag -f ${TAG_NAME} ${BUILDKITE_COMMIT}
git push -f origin ${TAG_NAME}

echo "--- Tag and push Docker image manifest"
GIT_ABBREV_COMMIT="${IMAGE_TAG:-git-${BUILDKITE_COMMIT:0:12}}"
DOCKER_IMAGE=docker.elastic.co/elasticsearch-ci/elasticsearch-serverless
X86_IMAGE_TAG=${DOCKER_IMAGE}:${GIT_ABBREV_COMMIT}-x86_64
ARM_IMAGE_TAG=${DOCKER_IMAGE}:${GIT_ABBREV_COMMIT}-aarch64

push_docker_manifest ${DOCKER_IMAGE}:current_${GITOPS_ENV} ${X86_IMAGE_TAG} ${ARM_IMAGE_TAG}
if [ "${GITOPS_ENV}" == "dev" ]; then
  # The "latest" tag always points to the latest dev version
  push_docker_manifest ${DOCKER_IMAGE}:latest ${X86_IMAGE_TAG} ${ARM_IMAGE_TAG}
fi
