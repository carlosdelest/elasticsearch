#!/bin/bash
set -e

# Validate that elasticsearch submodule commit references a commit in 'main' branch.
# This is primarily to ensure we don't accidentally push submodule updates referencing commits in branches or forks
SUBMODULE_COMMIT=$(git -C elasticsearch rev-parse HEAD)
BRANCH_TO_COMPARE=main
if [[ "$BUILDKITE_BRANCH" == "lucene_snapshot" ]]; then
  BRANCH_TO_COMPARE=lucene_snapshot
fi

if git -C elasticsearch merge-base --is-ancestor "$SUBMODULE_COMMIT" "origin/$BRANCH_TO_COMPARE"; then
  exit 0
else
  echo "Elasticsearch submodule commit '${SUBMODULE_COMMIT}' does not reference a commit in '${BRANCH_TO_COMPARE}' branch."
  exit 1
fi
