#!/bin/bash

set -euo pipefail

SKIP_INTAKE_TESTS="${SKIP_INTAKE_TESTS:-}"

{
  if [[ "$BUILDKITE_BRANCH" == "patch/serverless-fix-test" ]]; then
    # Parse PR number out of the commit message
    if [[ "$(uname)" == "Darwin" ]]; then
      PR_NUMBER=$(git log -1 --pretty=%B "$BUILDKITE_COMMIT" | grep -oE '\(#\d+\)' | cut -c2- | grep -oE '\d+' || true)
    else
      PR_NUMBER=$(git log -1 --pretty=%B "$BUILDKITE_COMMIT" | grep -oP '\(#\d+\)' | cut -c2- | grep -oP '\d+' || true)
    fi

    # Check if there's a difference in the actual source code between this commit and the PR commit
    DIFF=""
    if [[ -n "$PR_NUMBER" ]]; then
      git fetch origin "refs/pull/$PR_NUMBER/head:pull/$PR_NUMBER"
      DIFF=$(git diff --name-only "$BUILDKITE_COMMIT" "pull/$PR_NUMBER")
    fi

    if [[ -z "$PR_NUMBER" || -n "$DIFF" ]]; then
      if [[ -n "$PR_NUMBER" ]]; then
        echo "Changes found in the following files, will trigger full intake:"
        git diff --name-only "$BUILDKITE_COMMIT" "pull/$PR_NUMBER"
      fi
    else
      SKIP_INTAKE_TESTS="true"
    fi
  fi
} >&2

envsubst '$SKIP_INTAKE_TESTS' < .buildkite/pipeline.intake.yml
