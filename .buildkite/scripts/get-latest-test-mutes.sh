#!/bin/bash

testMuteFileEs="$(mktemp)"
testMuteFileServerless="$(mktemp)"

mutedTests="elasticsearch/muted-tests.yml"

# If this PR contains changes to muted-tests.yml, we disable this functionality
# Otherwise, we wouldn't be able to test unmutes
if [[ ! $(gh pr diff "$BUILDKITE_PULL_REQUEST" --name-only | grep 'muted-tests.yml') ]]; then
  gh api -H 'Accept: application/vnd.github.v3.raw' "repos/elastic/elasticsearch-serverless/contents/muted-tests.yml?ref=main" > "$testMuteFileServerless"
  gh api -H 'Accept: application/vnd.github.v3.raw' "repos/elastic/elasticsearch/contents/muted-tests.yml?ref=main" > "$testMuteFileEs"

  if [[ -s "$testMuteFileServerless" ]]; then
    mutedTests="$mutedTests,$testMuteFileServerless"
  fi

  if [[ -s "$testMuteFileEs" ]]; then
    mutedTests="$mutedTests,$testMuteFileEs"
  fi

  export GRADLE_OPTS="${GRADLE_OPTS:-} -Dorg.gradle.project.org.elasticsearch.additional.muted.tests=$mutedTests"
fi
