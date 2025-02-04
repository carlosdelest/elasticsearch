#!/bin/bash

ENTITLEMENTS_TESTING_ENABLED=''
if [[ "${ELASTICSEARCH_PR_NUMBER:-}" ]]; then
  LABELS=$(curl -s -H "Authorization: Bearer $GITHUB_TOKEN" "https://api.github.com/repos/elastic/elasticsearch/pulls/$ELASTICSEARCH_PR_NUMBER" | jq -r '.labels[].name')
  if [[ "$LABELS" == *"test-entitlements"* ]]; then
    ENTITLEMENTS_TESTING_ENABLED=true
  fi
fi
export ENTITLEMENTS_TESTING_ENABLED
