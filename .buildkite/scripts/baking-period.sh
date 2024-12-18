#!/bin/bash

set -e

scripts_dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )

if [[ $(buildkite-agent meta-data get "rerun" --default "false") == "true" ]]; then
    echo "Skipping bake period as this is a rerun"
    buildkite-agent step update "label" ":hourglass: Wait 0 minutes for environment health metrics"
else
    echo "Waiting for 30m for indicative health metrics"
    sleep ${GATE_WAIT_PERIOD}
fi