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

source "$BUILDKITE_DIR/scripts/utils/gke.sh"

echo $(gcloud --version)

gke_get_cluster_credentials $GCLOUD_SERVICE_ACCOUNT_VAULT_PATH $GCLOUD_PROJECT $GKE_CLUSTER_NAME $GCLOUD_REGION

if ! kubectl get namespace | grep -q "^$GCLOUD_ESS_DEV_NAMESPACE ";then
    echo "--- Creating namespace $GCLOUD_ESS_DEV_NAMESPACE"
    kubectl create namespace $GCLOUD_ESS_DEV_NAMESPACE
    kubectl create secret generic gcs-credentials --from-file=credentials_file=$GCLOUD_KEY_FILE -n $GCLOUD_ESS_DEV_NAMESPACE
else
   echo "namespace $GCLOUD_ESS_DEV_NAMESPACE exists"
fi

if kubectl get elasticsearchappconfig -n $GCLOUD_ESS_DEV_NAMESPACE | grep -q "^es ";then
  echo '--- Delete existing elasticsearchappconfig'
  kubectl delete elasticsearchappconfig es -n $GCLOUD_ESS_DEV_NAMESPACE
  kubectl wait --for delete pod --selector=elasticsearch.k8s.elastic.co/tier=search -n $GCLOUD_ESS_DEV_NAMESPACE --timeout=90s
  kubectl wait --for delete pod --selector=elasticsearch.k8s.elastic.co/tier=index -n $GCLOUD_ESS_DEV_NAMESPACE --timeout=90s
  kubectl wait --for delete pod --selector=elasticsearch.k8s.elastic.co/tier=master -n $GCLOUD_ESS_DEV_NAMESPACE --timeout=90s
fi

echo '--- Create GCS bucket to be used by ess'
# Using gcloud storage blocked by https://github.com/elastic/ci-agent-images/pull/196
# gcloud storage buckets create gs://$GCS_BUCKET
# gcloud storage buckets update gs://$GCS_BUCKET --update-labels=USAGE=ess-dev-test
gsutil ls -b gs://$GCS_BUCKET || gsutil mb -c standard gs://$GCS_BUCKET

# TODO fix labeling issue (see https://buildkite.com/elastic/elasticsearch-serverless-deploy-dev/builds/33#01887131-61cd-4511-b8cf-6d1f98180688)
# gsutil label ch -l usage:ess-dev-test_1 gs://$GCS_BUCKET


# Update config
echo '--- Apply ElasticsearchAppConfg yaml'

yq eval '.spec.objectStore.gcs.bucket = strenv(GCS_BUCKET) | 
         .spec.image = strenv(INTEG_TEST_IMAGE) |
         .metadata.labels."k8s.elastic.co/project-id" = strenv(K8S_DEPLOY_PROJECT_ID) |
         .metadata.labels."k8s.elastic.co/deployment-id" = strenv(K8S_DEPLOY_ID)
        ' $BUILDKITE_DIR/steps/k8s/ess-integtest-appconfig.yaml > $BUILDKITE_DIR/ess-config.yaml

kubectl apply -f $BUILDKITE_DIR/ess-config.yaml -n $GCLOUD_ESS_DEV_NAMESPACE

# Resources may not be found yet immediately
# TODO make that smarter
sleep 5
echo '--- Wait for ess pods be ready'
kubectl wait --for=condition=Ready pods --all -n $GCLOUD_ESS_DEV_NAMESPACE --timeout=180s

echo "--- Testing ess access"
ES_USERNAME=$(vault read -field username secret/ci/elastic-elasticsearch-serverless/gcloud-integtest-dev-ess-credentials)
ES_PASSWORD=$(vault read -field password secret/ci/elastic-elasticsearch-serverless/gcloud-integtest-dev-ess-credentials)
PUBLIC_IP=$(kubectl get svc ess-integtest-dev-proxy -n elastic-system -o json | jq -r '.status.loadBalancer.ingress[0].ip')

curl -k -u $ES_USERNAME:$ES_PASSWORD https://$K8S_DEPLOY_PROJECT_ID.es.$PUBLIC_IP.ip.es.io

buildkite-agent meta-data set "ess-pubic-url" "https://$K8S_DEPLOY_PROJECT_ID.es.$PUBLIC_IP.ip.es.io"
echo "Elasticsearch cluster available at https://$K8S_DEPLOY_PROJECT_ID.es.$PUBLIC_IP.ip.es.io" | buildkite-agent annotate --style "info" --context "ess-public-url"