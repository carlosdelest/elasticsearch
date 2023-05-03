#!/bin/bash
set -e
scripts_dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )

source $scripts_dir/utils/docker.sh

echo "--- Build serverless docker images"
source $scripts_dir/run-gradle.sh buildDockerImage buildAarch64DockerImage

echo "--- Tag and push docker images and manifest"
export GIT_ABBREV_COMMIT=git-${BUILDKITE_COMMIT:0:12}
export DOCKER_IMAGE=docker.elastic.co/elasticsearch-ci/elasticsearch-serverless
export X86_IMAGE_TAG=${DOCKER_IMAGE}:${GIT_ABBREV_COMMIT}-x86_64
export ARM_IMAGE_TAG=${DOCKER_IMAGE}:${GIT_ABBREV_COMMIT}-aarch64

docker tag elasticsearch-serverless:x86_64 ${X86_IMAGE_TAG}
docker tag elasticsearch-serverless:aarch64 ${ARM_IMAGE_TAG}
docker_login
docker push ${X86_IMAGE_TAG}
docker push ${ARM_IMAGE_TAG}
push_docker_manifest ${DOCKER_IMAGE}:${GIT_ABBREV_COMMIT} ${X86_IMAGE_TAG} ${ARM_IMAGE_TAG}
