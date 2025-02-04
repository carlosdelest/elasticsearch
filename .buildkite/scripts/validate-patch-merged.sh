#!/bin/bash
set -e

# Validate that changes from the patch branch have been merged to the commit which is being promoted

# Name of the branches in elasticsearch and elasticsearch-serverless to be prepared for the patch release
PATCH_BRANCH='patch/serverless-fix'

# We only want this validation for main branch promotion, because it may be expected to not have patch merged on other branches
if [[ ${BUILDKITE_BRANCH} != "main" || "$BLOCK_ON_PATCH_BRANCH_NOT_MERGED" == "false" ]]; then
  echo "Ignoring validation of changes from the '$PATCH_BRANCH' branch have been merged"
  exit 0
fi

echo "--- [elasticsearch-serverless] Validating changes from the '$PATCH_BRANCH' branch have been merged to '$BUILDKITE_BRANCH'"
git fetch origin $PATCH_BRANCH:$PATCH_BRANCH
if [[ -n $(git --no-pager log $PATCH_BRANCH --not $PROMOTED_COMMIT) ]]; then
  git --no-pager log $PATCH_BRANCH --not $PROMOTED_COMMIT
  echo "^^^ +++"
  ERROR_MESSAGE=":warning: There are unmerged changes in the '$PATCH_BRANCH' branch in elasticsearch-serverless"
  cat << EOF | buildkite-agent annotate --style "error" --context "validate-patch-merged"
$ERROR_MESSAGE

To fix this:

1. Merge the changes from the '$PATCH_BRANCH' branch to the '$BUILDKITE_BRANCH' branch in elasticsearch-serverless
2. Run the elasticsearch-serverless intake pipeline for '$BUILDKITE_BRANCH'
3. Re-run this promotion pipeline
EOF
  exit 1
else
  echo "All changes from '$PATCH_BRANCH' have been merged to '$BUILDKITE_BRANCH' and are included in '$PROMOTED_COMMIT'"
fi

echo "--- [elasticsearch] Validating changes from the '$PATCH_BRANCH' branch have been merged to '$BUILDKITE_BRANCH'"
ELASTICSEARCH_PROMOTED_COMMIT=$(git rev-parse $PROMOTED_COMMIT:elasticsearch)
git -C elasticsearch fetch origin $PATCH_BRANCH:$PATCH_BRANCH
if [[ -n $(git --no-pager -C elasticsearch log $PATCH_BRANCH --not $ELASTICSEARCH_PROMOTED_COMMIT) ]]; then
  git --no-pager -C elasticsearch log $PATCH_BRANCH --not $ELASTICSEARCH_PROMOTED_COMMIT
  echo "^^^ +++"
  ERROR_MESSAGE=":warning: There are unmerged changes in the '$PATCH_BRANCH' branch in elasticsearch"
  echo -e $ERROR_MESSAGE
  cat << EOF | buildkite-agent annotate --style "error" --context "validate-patch-merged"
$ERROR_MESSAGE

To fix this:

1. Merge the changes from the '$PATCH_BRANCH' branch to the '$BUILDKITE_BRANCH' branch in elasticsearch
2. Update the \`elasticsearch\` submodule in \`elasticsearch-serverless\` to the new commit
3. Run the elasticsearch-serverless intake pipeline for '$BUILDKITE_BRANCH'
4. Re-run this promotion pipeline
EOF
  exit 1
else
  echo "All changes from '$PATCH_BRANCH' have been merged to '$BUILDKITE_BRANCH' and are included in '$ELASTICSEARCH_PROMOTED_COMMIT'"
fi
