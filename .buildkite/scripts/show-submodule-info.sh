#!/bin/bash
set -e

# check elasticsearch submodule commit and show it in buildkite ui.
SUBMODULE_COMMIT=$(git -C elasticsearch rev-parse HEAD)

cat << EOF | buildkite-agent annotate --style "info" --priority 10 --context "submodule-info"
  <a href="https://github.com/elastic/elasticsearch/tree/${SUBMODULE_COMMIT}">Elasticsearch revision ${SUBMODULE_COMMIT}</a>
EOF
exit 0
