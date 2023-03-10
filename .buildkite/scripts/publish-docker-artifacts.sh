#!/bin/bash
set -e
scripts_dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )

echo "--- Build serverless docker images"
source $scripts_dir/run-gradle.sh buildDockerImage buildAarch64DockerImage

echo "--- Tag and push docker images and manifest"
set +x
export GIT_ABBREV_COMMIT=git-${BUILDKITE_COMMIT:0:12}
export DOCKER_REGISTRY_USERNAME="$(vault read -field=username secret/ci/elastic-elasticsearch-serverless/prod_docker_registry_credentials)"
export DOCKER_REGISTRY_PASSWORD="$(vault read -field=password secret/ci/elastic-elasticsearch-serverless/prod_docker_registry_credentials)"
export DOCKER_IMAGE=docker.elastic.co/elasticsearch-ci/elasticsearch-serverless
export X86_IMAGE_TAG=${DOCKER_IMAGE}:${GIT_ABBREV_COMMIT}-x86_64
export ARM_IMAGE_TAG=${DOCKER_IMAGE}:${GIT_ABBREV_COMMIT}-aarch64
docker tag elasticsearch-serverless:x86_64 ${X86_IMAGE_TAG}
docker tag elasticsearch-serverless:aarch64 ${ARM_IMAGE_TAG}
echo $DOCKER_REGISTRY_PASSWORD | docker login -u $DOCKER_REGISTRY_USERNAME --password-stdin docker.elastic.co
unset DOCKER_REGISTRY_USERNAME DOCKER_REGISTRY_PASSWORD
set -x
docker push ${X86_IMAGE_TAG}
docker push ${ARM_IMAGE_TAG}
docker manifest create ${DOCKER_IMAGE}:${GIT_ABBREV_COMMIT} --amend ${X86_IMAGE_TAG} --amend ${ARM_IMAGE_TAG}
docker manifest create ${DOCKER_IMAGE}:latest --amend ${X86_IMAGE_TAG} --amend ${ARM_IMAGE_TAG}
docker manifest push ${DOCKER_IMAGE}:${GIT_ABBREV_COMMIT}
docker manifest push ${DOCKER_IMAGE}:latest
