# Serverless / Stateless

### Motivation

Serverless concepts partly came out of the March 2022 offsite wherein brainstorming was done to come up
with ways to make kubernetes management of an elasticsearch cluster easier.

The difference between stateless and serverless
- Stateless is where data storage and data compute are separated. A user could self-manage a kubernetes
cluster of elasticsearch nodes along with a backing object store
- Serverless abstracts away all the elasticsearch management. The user ideally doesn't need to know
anything about it: it just works. Things like provisioning decisions, autoscaling, error handling, are
all done without the user's involvement. Users are also charged based on shared resource usage, rather
than for each server provisioned specially for a single user that has a price regardless of usage.

### Stateless Plugin

Stateless is implemented as an [elasticsearch plugin][], allowing stateless to override elasticsearch
functionality as necessary. Many of the additional stateless components can be [seen instantiated here][].
There are [two varieties][] of stateless nodes, either an index node or a search node. An index node can
be thought of as a primary shard that only accepts write; while a search node can be thought of as a
replica shard serving reads. Search and index node tiers are scaled separately.

[elasticsearch plugin]: https://github.com/elastic/elasticsearch-serverless/blob/f0e531703d1a1c4425e5caa3a317983c102c16c2/modules/stateless/src/main/java/co/elastic/elasticsearch/stateless/Stateless.java#L188
[seen instantiated here]: https://github.com/elastic/elasticsearch-serverless/blob/f0e531703d1a1c4425e5caa3a317983c102c16c2/modules/stateless/src/main/java/co/elastic/elasticsearch/stateless/Stateless.java#L210-L220
[two varieties]: https://github.com/elastic/elasticsearch-serverless/blob/f0e531703d1a1c4425e5caa3a317983c102c16c2/modules/stateless/src/main/java/co/elastic/elasticsearch/stateless/Stateless.java#L208

### Stateless Master Election

Every stateless index node is master eligible. Stateless runs an election by attempting to [compare and
set a register stored in the S3 object store][]. There are implementations of the [BlobContainer][]
class for each cloud provider that Elasticsearch supports. Each [BlobContainer][] implementation class
implements cloud specific functionality to atomically update the object store [LEASE_BLOB][]. A final
quorum of votes from master eligible nodes is not necessary for stateless master election: only
successfully writing to take over the object store lease.

Cluster state update operations behave as in a stateful elasticsearch cluster, broadcasting updates to
the cluster nodes in both the index and search tiers. Though the cluster state is persisted in the
object store (see [StatelessPersistedState][]) along with the rest of the persisted data.

[compare and set a register stored in the S3 object store]: https://github.com/elastic/elasticsearch-serverless/blob/f0e531703d1a1c4425e5caa3a317983c102c16c2/modules/stateless/src/main/java/co/elastic/elasticsearch/stateless/cluster/coordination/StatelessElectionStrategy.java#L105-L120
[BlobContainer]: https://github.com/elastic/elasticsearch/blob/8.10/server/src/main/java/org/elasticsearch/common/blobstore/BlobContainer.java#L25-L28
[LEASE_BLOB]: https://github.com/elastic/elasticsearch-serverless/blob/f0e531703d1a1c4425e5caa3a317983c102c16c2/modules/stateless/src/main/java/co/elastic/elasticsearch/stateless/cluster/coordination/StatelessElectionStrategy.java#L48
[StatelessPersistedState]: https://github.com/elastic/elasticsearch-serverless/blob/f0e531703d1a1c4425e5caa3a317983c102c16c2/modules/stateless/src/main/java/co/elastic/elasticsearch/stateless/cluster/coordination/StatelessPersistedState.java#L70

### Stateless Data Storage

The Amazon, Google and Azure object stores are fully managed services and will autoscale themselves as
load increases. Data replication, recovery, etc., is managed as part of those services.

### Stateless Shard Recovery

Stateless always recovers from the object store. A stateless elasticsearch instance will check for
any locally stored data structures for a shard in order to delete them before starting recovery.
Recovery creates an empty index writer and a new translog, first. Then the most recent commit to the
object store is fetched, along with the translog files. The translog is then replayed. Important to
note is that a stateless index node does not need to pull down all the shard data from the object
store: just the latest writes are cached locally.

### Stateless Indexing

Stateless overrides the [getEngineFactory()][] function to return an [IndexEngine][], which extends
the InternalEngine and provides write access to the remote object store. Stateless syncs the translog
differently than stateful. IndexEngine overrides [asyncEnsureTranslogSynced][] and writes to a new
TranslogReplicator component built for stateless. The [TranslogReplicator][] buffers writes locally
on the index node in order to minimize the number of writes out to the object store (each call to the
object store costs the user money). Writes across shards and indexes are all buffered in the
TranslogReplicator until a certain time period elapses or a data size threshold is met, then the
writes are pushed out to the object store and the search nodes are notified of the new commit.

The [TranslogReplicator.sync][] call made by the IndexEngine will wait for the TranslogReplicator to
commit to the object store before returning, so it can take a while to confirm a write. There is also
a [ValidateClusterStateTask][] that is responsible for checking on write to the object store that the
node is still a member of the cluster. ValidateClusterStateTask does this by checking [Lease][]
information, persisted in the object store, containing the current master term and 'node left generation'
numbers. The 'node left generation' number is incremented whenever a node is removed from the cluster
state for any reason. This handles the scenario where a node is unaware that is has been removed from
the cluster due to network issues and the master has already reassigned the node's shards to other nodes
in the cluster.

TODO: Index node data cache class. Actually, the index and search nodes both have the same cache,
should communicate that

[getEngineFactory()]: https://github.com/elastic/elasticsearch-serverless/blob/f0e531703d1a1c4425e5caa3a317983c102c16c2/modules/stateless/src/main/java/co/elastic/elasticsearch/stateless/Stateless.java#L841C36-L841C52
[IndexEngine]: https://github.com/elastic/elasticsearch-serverless/blob/f0e531703d1a1c4425e5caa3a317983c102c16c2/modules/stateless/src/main/java/co/elastic/elasticsearch/stateless/engine/IndexEngine.java#L57
[asyncEnsureTranslogSynced]: https://github.com/elastic/elasticsearch-serverless/blob/f0e531703d1a1c4425e5caa3a317983c102c16c2/modules/stateless/src/main/java/co/elastic/elasticsearch/stateless/engine/IndexEngine.java#L248
[TranslogReplicator]: https://github.com/elastic/elasticsearch-serverless/blob/f0e531703d1a1c4425e5caa3a317983c102c16c2/modules/stateless/src/main/java/co/elastic/elasticsearch/stateless/engine/translog/TranslogReplicator.java#L75
[TranslogReplicator.sync]: https://github.com/elastic/elasticsearch-serverless/blob/f0e531703d1a1c4425e5caa3a317983c102c16c2/modules/stateless/src/main/java/co/elastic/elasticsearch/stateless/engine/IndexEngine.java#L253
[ValidateClusterStateTask]: https://github.com/elastic/elasticsearch-serverless/blob/f0e531703d1a1c4425e5caa3a317983c102c16c2/modules/stateless/src/main/java/co/elastic/elasticsearch/stateless/engine/translog/TranslogReplicator.java#L440
[Lease]: https://github.com/elastic/elasticsearch-serverless/blob/f0e531703d1a1c4425e5caa3a317983c102c16c2/modules/stateless/src/main/java/co/elastic/elasticsearch/stateless/cluster/coordination/StatelessElectionStrategy.java#L246

### Stateless Search

Stateless search nodes never process cluster state updates. They just receive new cluster state from
the master.

### Stateless Data Refreshes

TODO

### Stateless Shard Movement

TODO

### Stateless Autoscaling

TODO

### Real-Time Get

See [`co/elastic/elasticsearch/stateless/engine/package-info.java`](https://github.com/elastic/elasticsearch-serverless/blob/main/modules/stateless/src/main/java/co/elastic/elasticsearch/stateless/engine/package-info.java)
