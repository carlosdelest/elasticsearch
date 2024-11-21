#!/bin/bash

set -eo pipefail

# If we're not setting a custom submodule commit, we can just use the serverless commit hash as the image tag as usual
# Otherwise, we can get multiple builds for the same commit hash, so let's use the build number as a unique suffix/tag instead
if [[ -z "$ELASTICSEARCH_SUBMODULE_COMMIT" ]]; then
  IMAGE_TAG_SUFFIX="${BUILDKITE_COMMIT:0:12}"
else
  # Note: The prefixes need to be of length 2 unless you update the padding length below
  PREFIX="pr"
  if [[ "$BUILDKITE_PIPELINE_SLUG" == "elasticsearch-serverless-es-pr-check" ]]; then
    PREFIX="es"
  fi

  # The image tag suffix needs to be 12 characters long, and unique per build
  # So lets just use the build number and pad it
  IMAGE_TAG_SUFFIX="${PREFIX}$(printf %10s "${BUILDKITE_BUILD_NUMBER}" | tr ' ' 0)"
fi

export IMAGE_TAG_SUFFIX
envsubst '$IMAGE_TAG_SUFFIX' < .buildkite/pipeline.pr.yml
