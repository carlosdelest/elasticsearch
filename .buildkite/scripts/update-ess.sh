#!/usr/bin/env bash

# ELASTICSEARCH CONFIDENTIAL
# __________________
#
#  Copyright Elasticsearch B.V. All rights reserved.
#
# NOTICE:  All information contained herein is, and remains
# the property of Elasticsearch B.V. and its suppliers, if any.
# The intellectual and technical concepts contained herein
# are proprietary to Elasticsearch B.V. and its suppliers and
# may be covered by U.S. and Foreign Patents, patents in
# process, and are protected by trade secret or copyright
# law.  Dissemination of this information or reproduction of
# this material is strictly forbidden unless prior written
# permission is obtained from Elasticsearch B.V.

set -euo pipefail

source "$BUILDKITE_DIR/scripts/utils/platform.sh"
source "$BUILDKITE_DIR/scripts/utils/misc.sh"

resolvePlatformEnvironment

DEPLOYMENT_NAME=$(curl -k -H "Authorization: ApiKey $API_KEY" \
     -H "Content-Type: application/json" "https://$PAPI_PUBLIC_IP:8443/api/v1/serverless/projects/elasticsearch/$PROJECT_ID" \
     | jq -r '.name')

GIVEN_ES_DEPLOYMENTS=$(kubectl get deployments -l "k8s.elastic.co/application-id=es" -n project-$PROJECT_ID -o json | jq -r -c '.items[].metadata')

checkForUpdatedDeployment() {
    result=0
    for given in $GIVEN_ES_DEPLOYMENTS; do
        deploymentName=$(echo $given | jq '.name')
        oldRevision=$(echo $given | jq -r '.annotations["deployment.kubernetes.io/revision"]')
        let expectedNewRevision=$oldRevision+1
        let currRevision=$(kubectl get deployments -l "k8s.elastic.co/application-id=es" -n project-$PROJECT_ID -o json | jq -r -c --arg deploymentName "es-es-index" '.items[] | select(.metadata.name == $deploymentName) | .metadata.annotations["deployment.kubernetes.io/revision"]')
        let readyReplicas=$(kubectl get deployments -l "k8s.elastic.co/application-id=es" -n project-$PROJECT_ID -o json | jq -r -c --arg deploymentName "es-es-index" '.items[] | select(.metadata.name == $deploymentName) | .status.replicas')
        echo "Deployment $deploymentName with old revision $oldRevision and expected new revision $expectedNewRevision is currently at revision $currRevision with ready replicas count $readyReplicas"
        if (($currRevision != $expectedNewRevision)) && [[ $readyReplicas -gt 0 ]]; then
            result=-1
        fi
    done
    return $result
}

echo "--- Updating deployment $PROJECT_ID - $DEPLOYMENT_NAME"
UPDATE_RESULT=$(curl -k -H "Authorization: ApiKey $API_KEY" \
     -H "Content-Type: application/json" "https://$PAPI_PUBLIC_IP:8443/api/v1/serverless/projects/elasticsearch/$PROJECT_ID" \
     -XPUT -d "{
        \"name\": \"$DEPLOYMENT_NAME\",
        \"overrides\": {
            \"elasticsearch\": {
                \"docker_image\": \"$INTEG_TEST_IMAGE\"
            }
        }
     }")

echo $UPDATE_RESULT
retry 20 10 checkForUpdatedDeployment
retry 20 1 "kubectl wait --for=condition=Ready pods --all -n project-$PROJECT_ID --timeout=240s"

echo "--- Testing ess access"
LBS_HOST=$(kubectl get svc proxy -n elastic-system -o json | jq -r '.status.loadBalancer.ingress[0].hostname')
curl -k -H "X-Found-Cluster: $PROJECT_ID.es" -H Authorization", "ApiKey $ESS_API_KEY" https://$LBS_HOST
