#!/bin/bash
set -e

# Get the list of dependent steps
dependentSteps=($(buildkite-agent step get "depends_on" --format json | jq -r '.[].step'))

# Iterate over the list of dependent steps and check if any of them failed
for step in "${dependentSteps[@]}"; do
  echo "Checking step $step"
  outcome="$(buildkite-agent step get "outcome" --step "${step}")"
  echo "outcome '$outcome'"
  if [[ -z "$outcome" ]]; then
    echo "Skipping empty or invalid step"
    continue
  fi
  if [[ "$outcome" != "passed" ]] && [[ "$outcome" != "broken" ]]; then
      echo "One or more quality gate steps ($step) failed ($outcome). Adding manual quality gate override."
       cat <<- YAML | buildkite-agent pipeline upload
     - group: ":judge: Manual Verification"
       steps:
         - label: ":pipeline: Upload manual step"
           command: "make -C /agent trigger-manual-verification-phase"
           agents:
             image: "docker.elastic.co/ci-agent-images/manual-verification-agent:0.0.4"
YAML
  break
  else
      echo "Step $step passed or not part of the pipeline. Continuing..."
  fi
done
