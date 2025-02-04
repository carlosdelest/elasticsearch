#!/bin/bash
set -e

# Validate that elasticsearch submodule commit references a commit in 'main' branch.
# This is primarily to ensure we don't accidentally push submodule updates referencing commits in branches or forks
SUBMODULE_COMMIT=$(git -C elasticsearch rev-parse HEAD)
BRANCH_TO_COMPARE=main

if [[ "$BUILDKITE_BRANCH" == "main" ]]; then
  if git -C elasticsearch merge-base --is-ancestor "$SUBMODULE_COMMIT" "origin/$BRANCH_TO_COMPARE"; then
    exit 0
  else
    echo "Elasticsearch submodule commit '${SUBMODULE_COMMIT}' does not reference a commit in '${BRANCH_TO_COMPARE}' branch."
    exit 1
  fi
fi

# For "patch/serverless-fix" branch check that elasticsearch submodule commit references the last commit from "patch/serverless-fix" branch
PATCH_BRANCH="patch/serverless-fix"
if [[ "$BUILDKITE_BRANCH" == "$PATCH_BRANCH" ]]; then
  git -C elasticsearch fetch origin $PATCH_BRANCH:$PATCH_BRANCH
  ELASTICSEARCH_PATCH_BRANCH_HEAD=$(git -C elasticsearch rev-parse $PATCH_BRANCH)
  if [[ "$SUBMODULE_COMMIT" == "$ELASTICSEARCH_PATCH_BRANCH_HEAD" ]]; then
    echo "Elasticsearch submodule commit '${SUBMODULE_COMMIT}' references the last commit from '${PATCH_BRANCH}' branch."
    exit 0
  else
    echo "Elasticsearch submodule commit '${SUBMODULE_COMMIT}' does not reference the last commit from '${PATCH_BRANCH}' branch."
    exit 1
  fi
fi
