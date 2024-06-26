# Distributed Area Team Internals

(Summary, brief discussion of our features)

# Networking

### ThreadPool

(We have many thread pools, what and why)

### ActionListener

See the [Javadocs for `ActionListener`](https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/action/ActionListener.java)

(TODO: add useful starter references and explanations for a range of Listener classes. Reference the Netty section.)

### REST Layer

The REST and Transport layers are bound together through the `ActionModule`. `ActionModule#initRestHandlers` registers all the
rest actions with a `RestController` that matches incoming requests to particular REST actions. `RestController#registerHandler`
uses each `Rest*Action`'s `#routes()` implementation to match HTTP requests to that particular `Rest*Action`. Typically, REST
actions follow the class naming convention `Rest*Action`, which makes them easier to find, but not always; the `#routes()`
definition can also be helpful in finding a REST action. `RestController#dispatchRequest` eventually calls `#handleRequest` on a
`RestHandler` implementation. `RestHandler` is the base class for `BaseRestHandler`, which most `Rest*Action` instances extend to
implement a particular REST action.

`BaseRestHandler#handleRequest` calls into `BaseRestHandler#prepareRequest`, which children `Rest*Action` classes extend to
define the behavior for a particular action. `RestController#dispatchRequest` passes a `RestChannel` to the `Rest*Action` via
`RestHandler#handleRequest`: `Rest*Action#prepareRequest` implementations return a `RestChannelConsumer` defining how to execute
the action and reply on the channel (usually in the form of completing an ActionListener wrapper). `Rest*Action#prepareRequest`
implementations are responsible for parsing the incoming request, and verifying that the structure of the request is valid.
`BaseRestHandler#handleRequest` will then check that all the request parameters have been consumed: unexpected request parameters
result in an error.

### How REST Actions Connect to Transport Actions

The Rest layer uses an implementation of `AbstractClient`. `BaseRestHandler#prepareRequest` takes a `NodeClient`: this client
knows how to connect to a specified TransportAction. A `Rest*Action` implementation will return a `RestChannelConsumer` that
most often invokes a call into a method on the `NodeClient` to pass through to the TransportAction. Along the way from
`BaseRestHandler#prepareRequest` through the `AbstractClient` and `NodeClient` code, `NodeClient#executeLocally` is called: this
method calls into `TaskManager#registerAndExecute`, registering the operation with the `TaskManager` so it can be found in Task
API requests, before moving on to execute the specified TransportAction.

`NodeClient` has a `NodeClient#actions` map from `ActionType` to `TransportAction`. `ActionModule#setupActions` registers all the
core TransportActions, as well as those defined in any plugins that are being used: plugins can override `Plugin#getActions()` to
define additional TransportActions. Note that not all TransportActions will be mapped back to a REST action: many TransportActions
are only used for internode operations/communications.

### Transport Layer

(Managed by the TransportService, TransportActions must be registered there, too)

(Executing a TransportAction (either locally via NodeClient or remotely via TransportService) is where most of the authorization & other security logic runs)

(What actions, and why, are registered in TransportService but not NodeClient?)

### Direct Node to Node Transport Layer

(TransportService maps incoming requests to TransportActions)

### Chunk Encoding

#### XContent

### Performance

### Netty

(long running actions should be forked off of the Netty thread. Keep short operations to avoid forking costs)

### Work Queues

### RestClient

The `RestClient` is primarily used in testing, to send requests against cluster nodes in the same format as would users. There
are some uses of `RestClient`, via `RestClientBuilder`, in the production code. For example, remote reindex leverages the
`RestClient` internally as the REST client to the remote elasticsearch cluster, and to take advantage of the compatibility of
`RestClient` requests with much older elasticsearch versions. The `RestClient` is also used externally by the `Java API Client`
to communicate with Elasticsearch.

# Cluster Coordination

(Sketch of important classes? Might inform more sections to add for details.)

(A NodeB can coordinate a search across several other nodes, when NodeB itself does not have the data, and then return a result to the caller. Explain this coordinating role)

### Node Roles

### Master Nodes

### Master Elections

(Quorum, terms, any eligibility limitations)

### Cluster Formation / Membership

(Explain joining, and how it happens every time a new master is elected)

#### Discovery

### Master Transport Actions

### Cluster State

#### Master Service

#### Cluster State Publication

(Majority concensus to apply, what happens if a master-eligible node falls behind / is incommunicado.)

#### Cluster State Application

(Go over the two kinds of listeners -- ClusterStateApplier and ClusterStateListener?)

#### Persistence

(Sketch ephemeral vs persisted cluster state.)

(what's the format for persisted metadata)

# Replication

(More Topics: ReplicationTracker concepts / highlights.)

### What is a Shard

### Primary Shard Selection

(How a primary shard is chosen)

#### Versioning

(terms and such)

### How Data Replicates

(How an index write replicates across shards -- TransportReplicationAction?)

### Consistency Guarantees

(What guarantees do we give the user about persistence and readability?)

# Locking

(rarely use locks)

### ShardLock

### Translog / Engine Locking

### Lucene Locking

# Engine

(What does Engine mean in the distrib layer? Distinguish Engine vs Directory vs Lucene)

(High level explanation of how translog ties in with Lucene)

(contrast Lucene vs ES flush / refresh / fsync)

### Refresh for Read

(internal vs external reader manager refreshes? flush vs refresh)

### Reference Counting

### Store

(Data lives beyond a high level IndexShard instance. Continue to exist until all references to the Store go away, then Lucene data is removed)

### Translog

(Explain checkpointing and generations, when happens on Lucene flush / fsync)

(Concurrency control for flushing)

(VersionMap)

#### Translog Truncation

#### Direct Translog Read

### Index Version

### Lucene

(copy a sketch of the files Lucene can have here and explain)

(Explain about SearchIndexInput -- IndexWriter, IndexReader -- and the shared blob cache)

(Lucene uses Directory, ES extends/overrides the Directory class to implement different forms of file storage.
Lucene contains a map of where all the data is located in files and offsites, and fetches it from various files.
ES doesn't just treat Lucene as a storage engine at the bottom (the end) of the stack. Rather ES has other information that
works in parallel with the storage engine.)

#### Segment Merges

# Recovery

(All shards go through a 'recovery' process. Describe high level. createShard goes through this code.)

(How is the translog involved in recovery?)

### Create a Shard

### Local Recovery

### Peer Recovery

### Snapshot Recovery

### Recovery Across Server Restart

(partial shard recoveries survive server restart? `reestablishRecovery`? How does that work.)

### How a Recovery Method is Chosen

# Data Tiers

(Frozen, warm, hot, etc.)

# Allocation

(AllocationService runs on the master node)

(Discuss different deciders that limit allocation. Sketch / list the different deciders that we have.)

### APIs for Balancing Operations

(Significant internal APIs for balancing a cluster)

### Heuristics for Allocation

### Cluster Reroute Command

(How does this command behave with the desired auto balancer.)

# Autoscaling

(Reactive and proactive autoscaling. Explain that we surface recommendations, how control plane uses it.)

(Sketch / list the different deciders that we have, and then also how we use information from each to make a recommendation.)

# Snapshot / Restore

(We've got some good package level documentation that should be linked here in the intro)

(copy a sketch of the file system here, with explanation -- good reference)

### Snapshot Repository

### Creation of a Snapshot

(Include an overview of the coordination between data and master nodes, which writes what and when)

(Concurrency control: generation numbers, pending generation number, etc.)

(partial snapshots)

### Deletion of a Snapshot

### Restoring a Snapshot

### Detecting Multiple Writers to a Single Repository

# Task Management / Tracking

(How we identify operations/tasks in the system and report upon them. How we group operations via parent task ID.)

### What Tasks Are Tracked

### Tracking A Task Across Threads

### Tracking A Task Across Nodes

### Kill / Cancel A Task

### Persistent Tasks

# Cross Cluster Replication (CCR)

(Brief explanation of the use case for CCR)

(Explain how this works at a high level, and details of any significant components / ideas.)

### Cross Cluster Search

# Indexing / CRUD

(Explain that the Distributed team is responsible for the write path, while the Search team owns the read path.)

(Generating document IDs. Same across shard replicas, \_id field)

(Sequence number: different than ID)

### Reindex

### Locking

(what limits write concurrency, and how do we minimize)

### Soft Deletes

### Refresh

(explain visibility of writes, and reference the Lucene section for more details (whatever makes more sense explained there))

# Server Startup

# Server Shutdown

### Closing a Shard

(this can also happen during shard reallocation, right? This might be a standalone topic, or need another section about it in allocation?...)
