#!/bin/bash

set -euo pipefail

if [[ "$BUILDKITE_BRANCH" != "lucene_snapshot"* ]]; then
  echo "Error: This script should only be run on lucene_snapshot branches"
  exit 1
fi

echo --- Updating "$BUILDKITE_BRANCH" branch with main

git config --global user.name elasticsearchmachine
git config --global user.email 'infra-root+elasticsearchmachine@elastic.co'

git fetch origin
git checkout "$BUILDKITE_BRANCH"
git reset --hard origin/"$BUILDKITE_BRANCH"
git merge --no-commit --no-ff origin/main || true

if [[ ! "$(git diff --cached)" ]]; then
  echo "Already up to date with main, nothing to do."
  exit 0
fi

git checkout "origin/$BUILDKITE_BRANCH" elasticsearch
if [[ $(git diff --check) != "" ]]; then
  echo "Cannot merge main. There are conflicting changes other than the elasticsearch submodule."
  git status
  exit 1
fi

git commit -m "Merge upstream/main into '$BUILDKITE_BRANCH'"
git push origin "$BUILDKITE_BRANCH"
