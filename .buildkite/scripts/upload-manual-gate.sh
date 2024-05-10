#!/bin/bash
set -e

QUALITY_GATE_STEP="${QUALITY_GATE_STEP:-quality-gates}"

if [ $(buildkite-agent step get "outcome" --step "${QUALITY_GATE_STEP}") != "passed" ]; then
  echo "Quality gates failed. Adding manual quality gate override."
    cat <<- YAML | buildkite-agent pipeline upload
  - group: ":judge: Manual Verification"
    steps:
      - label: ":pipeline: Upload manual step"
        command: "make -C /agent trigger-manual-verification-phase"
        agents:
          image: "docker.elastic.co/ci-agent-images/manual-verification-agent:0.0.4"
YAML
else
    echo "Quality gates passed. Continuing with deployment."
fi
