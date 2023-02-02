#!/usr/bin/env bash

echo "BUILDKITE_ARTIFACT_UPLOAD_DESTINATION: $BUILDKITE_ARTIFACT_UPLOAD_DESTINATION"
find . -type d -regex ".*build/reports" -print0 | tar -cvjf build-reports.tar.bz2 --null --files-from -
buildkite-agent artifact upload build-reports.tar.bz2
