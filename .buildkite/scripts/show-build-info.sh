#!/bin/bash
set -e

SUBMODULE_COMMIT=$(git -C elasticsearch rev-parse HEAD)

if [[ "${ES_SERVERLESS_IMAGE:-}" != "" ]]; then
  ES_SERVERLESS_IMAGE_INFO="**Serverless image**: \`${ES_SERVERLESS_IMAGE}\`"
fi

SUBMODULE_COMMIT_INFO="**Elasticsearch revision**: <a href=\"https://github.com/elastic/elasticsearch/tree/${SUBMODULE_COMMIT}\">${SUBMODULE_COMMIT}</a>"

cat << EOF | buildkite-agent annotate --style "info" --priority 10 --context "submodule-info"
  ${SUBMODULE_COMMIT_INFO}

  ${ES_SERVERLESS_IMAGE_INFO}
EOF
exit 0
