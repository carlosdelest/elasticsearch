#!/bin/bash
set -e

echo "Updating elasticsearch submodule to $(git -C elasticsearch rev-parse HEAD)"
git config user.name "elasticsearchmachine"
git config user.email "infra-root+elasticsearchmachine@elastic.co"
git add elasticsearch
git commit -m "Update elasticsearch submodule"
git pull --rebase
git push origin HEAD:${BUILDKITE_BRANCH}
