#!/bin/bash
set -euo pipefail

# The script retrieves promotion timestamp from Engineering Productivity
# "serverless-deployment-events" ESS cluster and writes into
# "elasticsearch-annotations" indices in O11y overview clusters (QA, Staging or
# Production).
#
# This is considered a temporary solution until
# https://elasticco.atlassian.net/browse/PF-58 is in. Once PF-58 is in,
# promotion annotations should be modified to read from local events data,
# instead of a custom "elasticsearch-annotations" index.
#
# Reference: https://elastic.slack.com/archives/C01DJ3DC0AX/p1718102951219139.

scripts_dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
source $scripts_dir/utils/misc.sh

VAULT_PREFIX="/secret/ci/elastic-elasticsearch-serverless"

echo "--- Show basic information"
echo "Environment: $ENVIRONMENT"
echo "Commit ID: $SERVICE_VERSION"

echo "--- Retrieve promotion timestamp"
data="$(vault_with_retries read -field=data -format=json $VAULT_PREFIX/esanno-deployment-events)"
DEPLOYMENT_EVENTS_ES_API_KEY="$(echo $data | jq -r .es_api_key)"
SEARCH_RESULT=$(curl -H "Authorization: ApiKey $DEPLOYMENT_EVENTS_ES_API_KEY" \
        -H "Content-Type: application/json" -XPOST -s \
        https://serverless-deployment-events.es.us-central1.gcp.cloud.es.io:9243/deployment-events/_search \
        -d "{
            \"size\": 1,
            \"query\": {
                \"bool\": {
                    \"filter\": [
                        {
                            \"term\": {
                                \"service.service_name.keyword\": \"elasticsearch\"
                            }
                        },
                        {
                            \"term\": {
                                \"service.phase.name.keyword\": \"gitops\"
                            }
                        },
                        {
                            \"term\": {
                                \"service.phase.state.keyword\": \"success\"
                            }
                        },
                        {
                            \"term\": {
                                \"service.environment.keyword\": \"$ENVIRONMENT\"
                            }
                        },
                        {
                            \"term\": {
                                \"service.hash.keyword\": \"$SERVICE_VERSION\"
                            }
                        }
                    ]
                }
            },
            \"sort\": [
                {
                    \"service.timestamp\": {
                        \"order\": \"desc\"
                    }
                }
            ]
        }")
PROMOTION_TIMESTAMP=$(echo $SEARCH_RESULT | jq -r '.hits.hits[0]._source.service.timestamp')
if [[ "$PROMOTION_TIMESTAMP" == "null" ]]; then
    echo "Promotion timestamp not found!"
    exit 1
fi
echo "Promotion timestamp: $PROMOTION_TIMESTAMP"

echo "--- Retrieve current timestamp"
CURRENT_TIMESTAMP=$(date --iso-8601=second)
echo "Current timestamp: $CURRENT_TIMESTAMP"

echo "--- Determine O11y cluster and Vault path"
# keep in sync with https://github.com/elastic/elasticsearch-serverless-support/blob/main/esanno-pyproject/src/esanno/conf.py
case $ENVIRONMENT in
    "qa")
        O11_ES_ENDPOINT="https://69fafb65a69b468689a58f523fc44de2.eu-west-1.aws.qa.cld.elstc.co:9243"
        O11_VAULT_PATH="esanno-qa.json"
        ;;
    "staging")
        O11_ES_ENDPOINT="https://f5840b8666994721a65f2900d02677b9.us-east-1.aws.staging.foundit.no:9243"
        O11_VAULT_PATH="esanno-staging.json"
        ;;
    "production")
        ;&
    "production-canary")
        ;&
    "production-noncanary")
        O11_ES_ENDPOINT="https://1abe339b8ee8411bacfda74fc62f1fca.us-east-1.aws.found.io:9243"
        O11_VAULT_PATH="esanno-prod.json"
        ;;
    *)
        echo "Unknown environment type, so won't annotate."
        exit 1
        ;;
esac
echo "ES endpoint: $O11_ES_ENDPOINT"
echo "Vault path: $VAULT_PREFIX/$O11_VAULT_PATH"

echo "--- Annotate"
data="$(vault_with_retries read -field=data -format=json $VAULT_PREFIX/$O11_VAULT_PATH)"
O11_ES_USERNAME="$(echo $data | jq -r .es_username)"
O11_ES_PASSWORD="$(echo $data | jq -r .es_password)"
if [ -z "${DEPLOYMENT_SLICES}" ]; then
    PROMO_TARGET_ENV="${$ENVIRONMENT}"
else
    PROMO_TARGET_ENV="${DEPLOYMENT_SLICES}"
fi
curl -u $O11_ES_USERNAME:$O11_ES_PASSWORD \
    -H "Content-Type: application/json" -XPOST \
    $O11_ES_ENDPOINT/elasticsearch-annotations/_doc \
    -d "{
        \"@timestamp\": \"$PROMOTION_TIMESTAMP\",
        \"type\": \"annotation\",
        \"annotation\": \"${SERVICE_VERSION:0:12} promotion in $PROMO_TARGET_ENV\",
        \"modified_by\": \"quality-gates\",
        \"modified_at\": \"$CURRENT_TIMESTAMP\"
    }"
echo
