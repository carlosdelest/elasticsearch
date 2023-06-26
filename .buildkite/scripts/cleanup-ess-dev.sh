#!/usr/bin/env bash

#
# ELASTICSEARCH CONFIDENTIAL
# __________________
#
# Copyright Elasticsearch B.V. All rights reserved.
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
#

set -euo pipefail

source "$BUILDKITE_DIR/scripts/utils/gke.sh"

gke_get_cluster_credentials $GCLOUD_SERVICE_ACCOUNT_VAULT_PATH $GCLOUD_PROJECT $GKE_CLUSTER_NAME $GCLOUD_REGION

epochNow=$(date "+%s")
epochHourNow=$((epochNow / 3600))

# get all namespaces that start with ess-dev-
namespaces=( $(kubectl get namespaces -o yaml | yq eval '.items.[].metadata.labels."kubernetes.io/metadata.name" | select(. == "ess-dev-*")' - ) )

# get length of namespaces array
namespaceslength=${#namespaces[@]}
for (( i=0; i<${namespaceslength}; i++ ));
do
  epochCreation=$(date -d $(kubectl get namespace ${namespaces[$i]} -o yaml | yq eval '.metadata.creationTimestamp' -) "+%s")
  epochHourCreation=$((epochCreation / 3600))
  ageInHour=$(($epochHourNow - $epochHourCreation))

  if (( ageInHour > $MAX_DEPLOYMENT_AGE_IN_HOUR )); then
      echo "Deleting deployment ${namespaces[$i]} ($ageInHour hours old)"
      . $BUILDKITE_DIR/scripts/undeploy-ess.sh ${namespaces[$i]} ${namespaces[$i]}-object-store
  else
      echo "Skipping deployment ${namespaces[$i]} ($ageInHour hours old)"
  fi
done

# cleanup dangling object stores
objectstores=( $(gsutil ls | grep "ess-dev-.*object-store") )
# get length of objectstores array
objectstoreslength=${#objectstores[@]}
for (( i=0; i<${objectstoreslength}; i++ ));
do
  echo "Checking objectstore at ${objectstores[$i]}"
  epochCreation=$(gsutil label get ${objectstores[$i]} | jq -r '."creation-timestamp"')
  epochHourCreation=$((epochCreation / 3600))
  ageInHour=$(($epochHourNow - $epochHourCreation))

  if (( ageInHour > $MAX_DEPLOYMENT_AGE_IN_HOUR )); then
      echo "Deleting dangling objectstore at ${objectstores[$i]} ($ageInHour hours old)"
      # delete related object store
      gsutil -m rm -r ${objectstores[$i]}
  fi
done
