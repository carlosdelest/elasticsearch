#!/bin/bash
set -e

utils_dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
source $utils_dir/misc.sh

# We read data from vault early to avoid vault token for ci job beeing expired later in the job
set +x
DOCKER_REGISTRY_USERNAME="$(vault_with_retries read -field=username secret/ci/elastic-elasticsearch-serverless/prod_docker_registry_credentials)"
DOCKER_REGISTRY_PASSWORD="$(vault_with_retries read -field=password secret/ci/elastic-elasticsearch-serverless/prod_docker_registry_credentials)"
set -x

docker_login() {
  set +x
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
