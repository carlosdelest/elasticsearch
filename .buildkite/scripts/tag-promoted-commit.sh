#!/bin/bash
set -exuo pipefail

echo "Tagging commit ${BUILDKITE_COMMIT}"
git config user.name "elasticsearchmachine"
git config user.email "infra-root+elasticsearchmachine@elastic.co"
git tag -f current_dev ${BUILDKITE_COMMIT}
git tag -f current_qa ${BUILDKITE_COMMIT}
git push -f origin current_dev current_qa
