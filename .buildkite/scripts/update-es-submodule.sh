#!/bin/bash
set -exuo pipefail

echo "Updating elasticsearch submodule to $(git -C elasticsearch rev-parse HEAD)"
git config user.name "elasticsearchmachine"
git config user.email "infra-root+elasticsearchmachine@elastic.co"
git add elasticsearch
if ! git diff-index --quiet --cached HEAD
then
  git commit -m "Update elasticsearch submodule"
  git pull --rebase origin ${BUILDKITE_BRANCH}
  git push origin HEAD:${BUILDKITE_BRANCH}
fi
