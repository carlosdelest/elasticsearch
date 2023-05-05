#!/bin/bash
set -e

docker_login() {
  set +x
  DOCKER_REGISTRY_USERNAME="$(vault read -field=username secret/ci/elastic-elasticsearch-serverless/prod_docker_registry_credentials)"
  DOCKER_REGISTRY_PASSWORD="$(vault read -field=password secret/ci/elastic-elasticsearch-serverless/prod_docker_registry_credentials)"
  echo $DOCKER_REGISTRY_PASSWORD | docker login -u $DOCKER_REGISTRY_USERNAME --password-stdin docker.elastic.co
  set -x
}

# Push docker manifest
# Usage: push_docker_manifest manifest_tag [amend_tag...]
push_docker_manifest() {
  additional_tags=""
  for tag in "${@:2}"; do
    additional_tags="${additional_tags} --amend ${tag} "
  done
  eval docker manifest create $1 ${additional_tags}
  docker_login
  docker manifest push $1
}
