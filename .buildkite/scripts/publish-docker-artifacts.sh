#!/bin/bash
set -e
scripts_dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )

source $scripts_dir/utils/docker.sh

GIT_ABBREV_COMMIT="${IMAGE_TAG:-git-${BUILDKITE_COMMIT:0:12}}"
DOCKER_IMAGE=docker.elastic.co/elasticsearch-ci/elasticsearch-serverless

# serverless docker images are build as release version
LICENSE_KEY=$(mktemp -d)/license.key
# Notice that only the public key is being read here, which isn't really secret
vault read -field pubkey secret/ci/elastic-elasticsearch-serverless/migrated/es-license | base64 --decode > "$LICENSE_KEY"
LICENSE_KEY_ARG="-Dlicense.key=$LICENSE_KEY"

GRADLE_PARAMS="-Dbuild.snapshot=false $LICENSE_KEY_ARG"

# by default we build and publish both x86_64 and aarch64 docker images
if [[ -z "${DEPLOY_ARCH}" ]]; then
    IMAGE_TAG=${DOCKER_IMAGE}:${GIT_ABBREV_COMMIT}
    echo "--- Check if docker image already exists"
    DOCKER_IMAGE_EXISTS=0
    docker manifest inspect $IMAGE_TAG || DOCKER_IMAGE_EXISTS=$?
    if [ $DOCKER_IMAGE_EXISTS -eq 1 ]; then
        echo "--- Build serverless docker images"
        source $scripts_dir/run-gradle.sh "$GRADLE_PARAMS" buildDockerImage buildAarch64DockerImage
        echo "--- Tag and push docker images and manifest"
        X86_IMAGE_TAG=${DOCKER_IMAGE}:${GIT_ABBREV_COMMIT}-x86_64
        ARM_IMAGE_TAG=${DOCKER_IMAGE}:${GIT_ABBREV_COMMIT}-aarch64
        docker tag elasticsearch-serverless:x86_64 ${X86_IMAGE_TAG}
        docker tag elasticsearch-serverless:aarch64 ${ARM_IMAGE_TAG}
        docker_login
        docker push ${X86_IMAGE_TAG}
        docker push ${ARM_IMAGE_TAG}
        push_docker_manifest ${DOCKER_IMAGE}:${GIT_ABBREV_COMMIT} ${X86_IMAGE_TAG} ${ARM_IMAGE_TAG}
        if [[ -n "${DOCKER_IMAGE_TAG}" ]]; then
          push_docker_manifest ${DOCKER_IMAGE}:${DOCKER_IMAGE_TAG} ${X86_IMAGE_TAG} ${ARM_IMAGE_TAG}
        fi
    else
        echo "--- Docker image $IMAGE_TAG already pubslished. Not rebuilding..."
    fi
# if specified we only build x86_64 to save time in the pipeline
elif [ "$DEPLOY_ARCH" == "x86_64" ]; then
    ARCH_IMAGE_TAG=${DOCKER_IMAGE}:${GIT_ABBREV_COMMIT}-x86_64
    echo "--- Check if docker image already exists"
    DOCKER_IMAGE_EXISTS=0
    docker manifest inspect $ARCH_IMAGE_TAG || DOCKER_IMAGE_EXISTS=$?
    if [ $DOCKER_IMAGE_EXISTS -eq 1 ]; then
        echo "--- Build serverless x86_64 docker image"
        source $scripts_dir/run-gradle.sh "$GRADLE_PARAMS" buildDockerImage
        echo "--- Tag and push docker image and manifest"

        docker tag elasticsearch-serverless:x86_64 ${ARCH_IMAGE_TAG}
        docker_login
        docker push ${ARCH_IMAGE_TAG}
        push_docker_manifest ${DOCKER_IMAGE}:${GIT_ABBREV_COMMIT} ${ARCH_IMAGE_TAG}
        if [[ -n "${DOCKER_IMAGE_TAG}" ]]; then
          push_docker_manifest ${DOCKER_IMAGE}:${DOCKER_IMAGE_TAG} ${ARCH_IMAGE_TAG}
        fi
    else
        echo "--- Docker image $ARCH_IMAGE_TAG already pubslished. Not rebuilding..."
    fi
fi
