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

undeploynamespace=${1:-$GCLOUD_ESS_DEV_NAMESPACE}
gcs_bucket=${2:-$GCS_BUCKET}
gke_get_cluster_credentials $GCLOUD_SERVICE_ACCOUNT_VAULT_PATH $GCLOUD_PROJECT $GKE_CLUSTER_NAME $GCLOUD_REGION

if kubectl get namespace | grep -q "^$undeploynamespace ";then
    echo "Deleting namespace $undeploynamespace"
    kubectl delete namespace $undeploynamespace
else
   echo "namespace $undeploynamespace does not exist"
fi

# create gcs bucket to be used
# using gcloud storage command blocked by https://github.com/elastic/ci-agent-images/pull/196
# using -m allows running deletion of objects in bucket in parallel
gsutil -m rm -r gs://$gcs_bucket
