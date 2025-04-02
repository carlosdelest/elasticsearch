#!/bin/bash
set -euo pipefail

# Name of the branches in elasticsearch and elasticsearch-serverless to be prepared for the patch release
PATCH_BRANCH='patch/serverless-fix'

git config --global user.name "elasticsearchmachine"
git config --global user.email "infra-root+elasticsearchmachine@elastic.co"

if [[ "$BUILDKITE_BRANCH" != "main" ]]; then
  echo "This script should be run only on the 'main' branch"
  exit 1
fi

check_merge_conflicts() {
    local target_branch=$1

    # Get the merge base
    local base=$(git merge-base $target_branch main)

    # Check for conflicts using merge-tree
    if git merge-tree $base main $target_branch | grep -q "^+<<<<<<<"; then
      return 1
    fi
    return 0
}

merge_patch_branch() {
  local repo_path=$1
  local promoted_commit=$2

  local repo_name
  repo_name=$(basename "$(realpath $repo_path)")
  local original_path
  original_path=$(pwd)

  cd "$repo_path" || exit

  git checkout main
  git pull origin main

  # Check if there are changes on the patch branch
  if [[ -z $(git --no-pager log $promoted_commit --not main) ]]; then
    echo "[$repo_name] No changes found on the $PATCH_BRANCH. Nothing to do."
    cd "$original_path" || exit
    return
  else
    echo "[$repo_name] Commits to be merged from '$PATCH_BRANCH' branch"
    git --no-pager log $promoted_commit --not main
  fi

  # Checkout the patch branch
  git fetch origin $PATCH_BRANCH:$PATCH_BRANCH

  # Check for merge conflicts
  if ! check_merge_conflicts $PATCH_BRANCH; then
    buildkite-agent annotate --style error --context "merge-patch-branches" \
":warning: There are merge conflicts when merging the $PATCH_BRANCH branch into main in $repo_name.
You need to resolve conflicts and merge the branch manually before retrying this job.

Instructions can be found <a href='https://docs.elastic.dev/elasticsearch-team/serverless/releases/patch-release'>here</a>."
    exit 1
  fi

  echo "[$repo_name] Merging $PATCH_BRANCH branch into main"
  # Merge patch branch
  git merge $PATCH_BRANCH --no-ff --no-commit

  # Commit and push
  git commit -m "Merge $PATCH_BRANCH into main"
  git push origin main
  echo "The $repo_name $PATCH_BRANCH branch has been merged into main"

  cd "$original_path" || exit
}

echo "--- [elasticsearch] Merging $PATCH_BRANCH branch into main"
ELASTICSEARCH_PROMOTED_COMMIT=$(git rev-parse $PROMOTED_COMMIT:elasticsearch)
merge_patch_branch elasticsearch $ELASTICSEARCH_PROMOTED_COMMIT

echo "--- Updating elasticsearch submodule in elasticsearch-serverless"
git add elasticsearch
git commit -m "Update elasticsearch submodule" || echo "No changes to the elasticsearch submodule in elasticsearch-serverless"

echo "--- [elasticsearch-serverless] Merging $PATCH_BRANCH branch into main"
merge_patch_branch . $PROMOTED_COMMIT

# Annotate the build with the information
buildkite-agent annotate --style success \
  --context "merge-patch-branches" \
  --priority 1 \
  "The $PATCH_BRANCH branches has been merged into main"
