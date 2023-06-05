#!/bin/bash
set -e

# Validate that elasticsearch submodule commit references a commit in 'main' branch.
# This is primarily to ensure we don't accidentally push submodule updates referencing commits in branches or forks
SUBMODULE_COMMIT=$(git -C elasticsearch rev-parse HEAD)

if git -C elasticsearch merge-base --is-ancestor ${SUBMODULE_COMMIT} origin/main; then
  exit 0
else
  echo "Elasticsearch submodule commit '${SUBMODULE_COMMIT}' does not reference a commit in 'main' branch."
  exit 1
fi
