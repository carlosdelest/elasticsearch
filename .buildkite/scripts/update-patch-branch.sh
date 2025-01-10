#!/bin/bash

# Reset the `patch-serverless` branches of elasticsearch and elasticsearch-serverless to the given commit
# It's executed after the release to QA, to make the branch ready for the possible future patch release

set -euo pipefail

# The target commit that was deployed to QA
TARGET_COMMIT=$1

# Name of the branches in elasticsearch and elasticsearch-serverless to be prepared for the patch release
PATCH_BRANCH='patch/serverless-fix'

git config user.name "elasticsearchmachine"
git config user.email "infra-root+elasticsearchmachine@elastic.co"

echo "--- Resetting the elasticsearch-serverless '$PATCH_BRANCH' branch to the commit '$TARGET_COMMIT'"
git branch -D $PATCH_BRANCH || true
git push origin --delete $PATCH_BRANCH || true
git branch $PATCH_BRANCH $TARGET_COMMIT
git push origin $PATCH_BRANCH
echo "The elasticsearch-serverless '$PATCH_BRANCH' branch has been reset to the commit '$TARGET_COMMIT'"

# Get the commit of the elasticsearch submodule
ELASTICSEARCH_COMMIT=$(git rev-parse $TARGET_COMMIT:elasticsearch)
echo "--- Resetting the elasticsearch '$PATCH_BRANCH' branch to the commit '$ELASTICSEARCH_COMMIT'"
cd elasticsearch
git branch -D $PATCH_BRANCH || true
git push origin --delete $PATCH_BRANCH || true
git branch $PATCH_BRANCH $ELASTICSEARCH_COMMIT
git push origin $PATCH_BRANCH
echo "The elasticsearch '$PATCH_BRANCH' branch has been reset to the commit '$ELASTICSEARCH_COMMIT'"


