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

pip install azure-cosmos
python - <<'PY'
import json, os, time, socket
from azure.cosmos import CosmosClient, PartitionKey, exceptions
from azure.core.exceptions import ServiceRequestError

endpoint = os.environ["COSMOS_ENDPOINT"]   # e.g. https://localhost:8081
key      = os.environ["COSMOS_KEY"]
db_name  = os.environ["COSMOS_DATABASE"]
max_tries  = 30

host, port = endpoint.replace("https://", "").replace("http://", "").split(":")
print("waiting for Cosmos emulator socket …", flush=True)
print("connecting to %s:%s" % (host, port), flush=True)
for _ in range(max_tries):
    try:
        with socket.create_connection((host, int(port)), timeout=1):
            break
    except OSError:
        time.sleep(2)
else:
    raise RuntimeError("Cosmos DB socket never became reachable")

client = CosmosClient(endpoint, key, connection_verify=False)  # skip TLS check

for attempt in range(1, max_tries + 1):
    try:
        db = client.create_database_if_not_exists(db_name)
        break
    except (ServiceRequestError, exceptions.CosmosHttpResponseError, OSError):
        if attempt == max_tries:
            raise
        time.sleep(2)

print("database ready, creating containers …", flush=True)

api    = db.create_container_if_not_exists(
            id="api-keys",
            partition_key=PartitionKey(path="/id"),
            offer_throughput=400)

token = db.create_container_if_not_exists(
            id="token-invalidation",
            partition_key=PartitionKey(path="/id"))

users = db.create_container_if_not_exists(
            id="users",
            partition_key=PartitionKey(path="/id"))

with open("/api-keys.json") as f:
    for doc in json.load(f):
        api.upsert_item(doc)

print("Cosmos DB fully boot-strapped", flush=True)
PY
