# Elasticsearch - Metering plugin

<!--
Original doc: https://docs.google.com/document/d/10tHJIiuMEKZRsXzDdR9p8ULwaK7mnK6LEy4BWCg7bvs
-->

## Summary

This plugin adds functionalities to serverless Elasticsearch to meter data usage for billing purposes. It computes various metrics, accumulates them and sends them at regular intervals (by default, every 5 minutes) to the [Serverless metering pipeline](https://github.com/elastic/platform-billing/blob/main/teams/billing/services/serverless_onboarding.md#stages--responsibilities) via the REST-based [Usage API](https://github.com/elastic/platform-billing/blob/main/teams/billing/services/usage_record_schema_v2.md).

The metrics metered can be categorized into two types:

* sampled metrics (point-in-time quantities), implementing the `SampledMetricsProvider` interface,
* counter metrics (accumulators for monotonically increasing functions), implementing the `CounterMetricsProvider`.

**This plugin currently computes and reports three different metrics:**

| Metric name    | Description                           | Metric type | Usage type name      | Reporting granularity     | Project types                               |
|----------------|---------------------------------------|-------------|----------------------|---------------------------|---------------------------------------------|
| **RA-I**ngest  | raw ingested data in bytes            | counter     | `es_raw_data`        | per node and index        | general (unused*), O11y, Security           |
| **RA-S**torage | raw stored data in bytes              | sample      | `es_raw_stored_data` | per cluster and index     | O11y, Security                              |
| **IX**         | index size in bytes                   | sample      | `es_indexed_data`    | per cluster and shard     | general, O11y (unused*), Security (unused*) |
| **VCU**        | virtual compute units in bytes of RAM | sample      | `es_vcu`             | per tier (search / index) | general, O11y (unused*), Security (unused*) |

(*) Important: this table highlights what is computed and reported by the metering plugin; this may be different from what is actually consumed by the [metering pipeline](https://github.com/elastic/platform-billing/blob/main/teams/billing/services/serverless_onboarding.md#stages--responsibilities) and used for billing. For detailed and up-to-date documentation from a billing perspective, see the higher level business documentation: [PRD - Serverless monitoring](https://docs.google.com/document/d/1ILQHCrMSWFB403fJHI45jarolcOC4l6zlDoZbqKK0HA/edit#heading=h.7zjfkrex9jtg).


## Metric computation


### RA-Ingest

The normalized raw size of ingested data (RA-I) is metered when parsing the source of ingested documents before indexing. This normalized counter metric is metered in bytes and is independent of the actual XContent type used, making it independent of the actual size on the wire.

It is calculated as the sum of the size of all field names in addition to the respective field value size based on the actual data type in the source. The type mapping is not considered, disabled fields (skipped children) are metered as well.

Data types are treated as follows (for further details see [here](https://docs.google.com/document/d/1N2v8dZ4hyd_u7Q3suiRYDOKAwVR984JdffQADPmO7Kw/edit#heading=h.45ony4j9gyzn)):

| Data type   | Size in bytes                                                                                       | Notes                                                                    |
|-------------|-----------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------|
| text        | size of bytes using variable-width UTF-8 encoding, for ASCII characters that’s 1 byte per character | applies also to field names and string encoded numbers or boolean values |
| number      | 8 bytes                                                                                             |                                                                          |
| boolean     | 1 byte                                                                                              |                                                                          |
| binary data | base 64 encoded text size                                                                           |                                                                          |

RA-I (and RA-S, described below) are applied differently based on the operation type: "regular" index/update, update-by-doc, or update-by-script.

For update-by-doc, the RA-I increment for the update (i.e. the normalized raw size of the ingested update document) is calculated before merging it with the source that is already present. Update-by-script does not affect RA-I, even if the script is inlined.

The ratio of these decisions, described in more detail in the [PRD document](https://docs.google.com/document/d/1ILQHCrMSWFB403fJHI45jarolcOC4l6zlDoZbqKK0HA/), is that RA-I measures  the "amount of data the customer sends to the cluster". In update-by-doc, the “data sent to the cluster” is the update document, in update-by-script, customers do not send any data to the cluster, they process existing data.

RA-I is reported _per node_. During document parsing we compute RA-I creating a per-document size. When the indexing of a document completes successfully, the per-document size is accumulated on the indexing node, in memory(**), incrementing a per-index counter. A dedicated component (`UsageReportCollector`, running on every node) calls the RA-I counter provider at a regular interval (by default 5 minutes) to create and publish a billing usage record from the per-index counters. As a final step we commit the operation to the counter provider, resetting the per-index counters. This is done only after `UsageReportCollector` has successfully published the usage record(s) to Usage API, so that - in case of failure - the counters are preserved and the operation can be retried at the next iteration.

(**) This means that if a node crashes we lose the data we have yet to report. Since RA-I is a counter (an accumulator for a monotonically increasing function), losing the partial data for the period means under-billing, which is the better alternative in case of error.

RA-I usage records look as follows:

```json
{
    "id": "ingested-doc:{index name}:{node id}-{current timestamp}",
    "usage_timestamp": {current timestamp},
    "usage": {
        "type": "es-raw-data",
        "period_seconds": {reporting period},
        "quantity": {RA-Ingest}
    },
    "source": {
        "id": "es-{node id}",
        "instance_group_id": "{project id}",
        "metadata": {
            "index": "{index name}"
        }
    }
}
```

Code references:

* [`XContentMeteringParser`](https://github.com/elastic/elasticsearch-serverless/blob/main/modules/serverless-metering/src/main/java/co/elastic/elasticsearch/metering/xcontent/XContentMeteringParser.java): Metering of RA-I while parsing a document source
* [`RAIngestMetricReporter`](https://github.com/elastic/elasticsearch-serverless/blob/main/modules/serverless-metering/src/main/java/co/elastic/elasticsearch/metering/ingested_size/reporter/RAIngestMetricReporter.java): Reports RA-I per doc to `IngestMetricsProvider` once indexing completed
* [`IngestMetricsProvider`](https://github.com/elastic/elasticsearch-serverless/blob/main/modules/serverless-metering/src/main/java/co/elastic/elasticsearch/metering/IngestMetricsProvider.java): Accumulates RA-I per index in memory and generates ingest usage metrics based on that
* [`UsageReportCollector`](https://github.com/elastic/elasticsearch-serverless/blob/main/modules/serverless-metering/src/main/java/co/elastic/elasticsearch/metering/usagereports/UsageReportCollector.java): Periodically collects usage metrics from all metrics providers and publishes those


### RA-Storage

The normalized raw stored size of data (RA-S) is based on the normalized raw size of ingested data (RA-I) computed when parsing the document source, as described above.

**Note:**
Closed indices are not included when metering RA-S. Closing an index requires Operator privileges and customers cannot do this themselves.
If doing so on a customer's behalf, be aware that we will stop charging the customer for that index.

#### Calculation of RA-S for timeseries indices

Calculation of RA-Storage for timeseries indices is much simpler than the calculation for regular indices, as deletes (and updates) do not have to be taken into account. The optimized calculation for timeseries indices is therefore described separately in this section.

Once indexing of a document completes successfully, the per-document RA-I size (equal to RA-S in this case) is accumulated per index shard, into an accumulator separate from the one used for RA-I. When a new segment is created at commit time, the RA-S delta captured by the accumulator is summed with the total RA-S attribute in the user data of the previous segment infos and written to the user data of the new commit (into the newly created segment infos).

For reporting, total RA-S of a shard is read from the above-mentioned attribute in the segment infos user data (of the latest generation of that shard).

Code references:
* [`RAStorageReporter`](https://github.com/elastic/elasticsearch-serverless/blob/main/modules/serverless-metering/src/main/java/co/elastic/elasticsearch/metering/ingested_size/reporter/RAStorageReporter.java): Reports RA-Storage per doc to the `RaStorageAccumulator`
* [`RaStorageAccumulator`](https://github.com/elastic/elasticsearch-serverless/blob/main/modules/serverless-metering/src/main/java/co/elastic/elasticsearch/metering/ingested_size/reporter/RAStorageAccumulator.java): Accumulates the RA-S delta of indexed documents since the last commit and provides the update commit user data.

#### Calculation of RA-S for regular indices

For regular indices we have to take deletes into account, which adds significant complexity.

Deleted documents might still be present in segments on disk, at least temporarily, until segments are merged and deleted documents are dried up. When using RA-S we do not want to charge customers for these. This is different from IX, where we report the disk size of indices, which includes the deleted documents still present in segments on disk.

We investigated various [implementation alternatives](https://docs.google.com/document/d/1DoDaFYFm-pvjIvQV-aLn2OdqrEyXGOd33ZcO8L8-3_c/edit#heading=h.b0pjniqkf5si) and decided for the following approach, which is the best compromise between correctness and performance (benchmarks of the various approaches can be found [here](https://docs.google.com/spreadsheets/d/1Df3jCoRgqLNmONhKfR9N1CqBn1kMhchI-Lm6fZithFQ/edit?gid=386942869#gid=386942869)).

This approach works by storing the average per-document RA-S size in the segment infos for each segment. Based on that value, we calculate an approximated “live” per-segment RA-S size taking deletions into account. We also store the per-document RA-S size as an additional internal (and hidden) field named `_rastorage`; that way we can recalculate the average when merging segments, so that the approximated value eventually becomes a precise value (when live documents = total documents).

The `_rastorage` field is added after parsing a document as a numeric DocValues field. We have to distinguish three cases here:
* For regular index operations including bulk requests, RA-S per document is equal to RA-I, regardless if a new document is inserted or an existing document is updated (an update in this case can be considered just like a delete + index).
* For updates by document, the source is parsed twice resulting in two separate sizes: Parsing of the original ingested update defines RA-I, parsing the final source after merging the update with the previous source defines RA-S of the updated document.
* Finally, for updates by script, no RA-I is metered. Parsing of the final source after applying the script to the previous version defines RA-S of the new document.


|                      | RA-I                            | RA-S                                 |
|----------------------|---------------------------------|--------------------------------------|
| **Index**            | Calculated                      | Calculated                           |
| **Update**           | Calculated                      | Calculated                           |
| **Update by doc**    | Calculated on the  “update” doc | Calculated on the final (merged) doc |
| **Update by script** | NOT calculated                  | Calculated on the final doc          |


When a new segment is created at commit time or segments are merged, the average RA-S per document of that segment is calculated using a “filter” `DocValuesConsumer` for the `_rastorage` field, which stores the per-doc average in a `_rastorage_avg` field attribute in the segment info.  \
Notice that this happens at commit time: the `DocValuesConsumer` reads the field we wrote on parsing completed when is about to be persisted, so we persist the field and the per-doc average only at commit time. This means that if indexing fails, the RA-S contribution is not metered.

For reporting, total RA-S of a shard can then be calculated by summing the approximate live RA-S of each segment, which is the product of the count of live documents in the segment and the average RA-S per document of that segment. If no deletions are present in a segment, e.g.  every time after merging segments, the calculated RA-S of that segment will be precise (again).

RA-S usage sample records look as follows:

```json
{
    "id": "raw-stored-index-size:{index name}-{sampling timestamp}",
    "usage_timestamp": {sampling timestamp},
    "usage": {
        "type": "es_raw_stored_data",
        "period_seconds": {reporting period},
        "quantity": {RA-Storage}
    },
    "source": {
        "id": "es-{node id}",
        "instance_group_id": "{project id}",
        "metadata": {
            "index": "{index name}"
        }
    }
}
```

Code references:
* [`RAStorageReporter`](https://github.com/elastic/elasticsearch-serverless/blob/main/modules/serverless-metering/src/main/java/co/elastic/elasticsearch/metering/ingested_size/reporter/RAStorageReporter.java): Writes RA-S as additional, hidden field to each document
* [`RAStorageDocValuesFormatFactory`](https://github.com/elastic/elasticsearch-serverless/blob/main/modules/serverless-metering/src/main/java/co/elastic/elasticsearch/metering/codec/RAStorageDocValuesFormatFactory.java): Creates the `DocValuesConsumer` responsible for calculating the average RA-S per document of a segment

### IX (Index Size)

IX (index size) is a simpler storage metric, computed by summing the disk size occupied by segments on disk. Being based on the actual disk space occupied, this metric includes the deleted documents still present in segments on disk. The metric is sampled, so eventually when/if the deleted documents are cleaned up (e.g. after merging), the segments will occupy less space on disk, and we will report the new size at the next sample.

**Note:**
Closed indices are not included when metering IX. Closing an index requires Operator privileges and customers cannot do this themselves.
If doing so on a customer's behalf, be aware that we will stop charging the customer for that index.

IX usage sample records look as follows:

```json
{
    "id": "shard-size:{index name}:{shard id}-{sampling timestamp}",
    "usage_timestamp": {sampling timestamp},
    "usage": {
        "type": "es_indexed_data",
        "period_seconds": {reporting period},
        "quantity": {IX}
    },
    "source": {
        "id": "es-{node id}",
        "instance_group_id": "{project id}",
        "metadata": {
            "index": "{index name}",
            "shard": "{shard id}"
        }
    }
}
```

### VCU (virtual compute units) in bytes of RAM

The VCU calculation for serverless is memory based. VCU usage records of type `es_vcu` report the provisioned amount of memory in bytes per tier (search, index) which is the sum of the provisioned memory of nodes in the respective tier.

Additionally, tier activity is reported to be able to charge differently for periods of inactivity. Activity is captured on each node and only non-operator activity is considered.
While some actions activate only the assigned tier of a node, other, more general actions (such as a refresh or settings update) activate both tiers. Once activated, a tier remains active for at least the cooldown period of 15 minutes.
The cluster sampling infrastructure then builds a cluster wide consolidated per tier view of current activity, for details see the next section below.

`es_vcu` contain the following additional usage metadata in `usage.metadata`:
- `application_tier`: the tier (`search` or `index`)
- `active`: if the tier is active during the current sampling period
- `latest_activity_timestamp`: the timestamp (ISO-8601 formatted) of the last activity of that tier
- `sp_min_provisioned_memory`: (search tier only) SP min provisioned RAM (bytes).\
- `sp_min`: (search tier only) minimum search power configured for the project.\
  Note: In certain error cases SP min provisioned RAM cannot be calculated and won't be added. very likely, the search tier is not operational in that case. This is actively monitored.

VCU usage sample records look as follows:

```json
{
    "id": "vcu:{tier}-{sampling timestamp}",
    "usage_timestamp": {sampling timestamp},
    "usage": {
        "type": "es_vcu",
        "period_seconds": {reporting period},
        "quantity": {VCU in bytes of RAM},
        "metadata": {
          "application_tier": "{search/index}",
          "active": "{true/false}",
          "latest_activity_timestamp": "{latest activity timestamp}",
          "sp_min_provisioned_memory": "{SP min provisioned RAM in bytes (search tier only)}",
          "sp_min": "{minimum search power configured for the project (search tier only)}"
        }
    },
    "source": {
        "id": "es-{node id}",
        "instance_group_id": "{project id}"
    }
}
```

Code references:
* [`TaskActivityTracker`](https://github.com/elastic/elasticsearch-serverless/blob/main/modules/serverless-metering/src/main/java/co/elastic/elasticsearch/metering/activitytracking/TaskActivityTracker.java): tracks activity per tier by means of an action filter ([`ActivityTrackerActionFilter`](https://github.com/elastic/elasticsearch-serverless/blob/main/modules/serverless-metering/src/main/java/co/elastic/elasticsearch/metering/activitytracking/ActivityTrackerActionFilter.java).
* [`SampledVCUMetricsProvider`](https://github.com/elastic/elasticsearch-serverless/blob/d7992ec3b4a7e140f8421509f4ed46c4605f9037/modules/serverless-metering/src/main/java/co/elastic/elasticsearch/metering/sampling/SampledVCUMetricsProvider.java): provides per tier VCU samples

### Per cluster reporting infrastructure for RA-S and IX usage sample records

Differently from RA-I, sampled metrics such as RA-S, IX but also VCU are reported _per cluster_.  \
Each cluster has a dedicated “sampling” node, designated by means of a persistent task. This persistent task periodically triggers a service (`SampledClusterMetricsService`) to collect updated samples from all nodes in the cluster such as memory or activity (per application tier).\
Additionally, search nodes provide per-shard information on IX and RA-S (plus other information, like doc count), computed by iterating over all indices/shards/segments on that node and summing up the stats we find in the (latest) segment commit info (for details, see the previous sections). Notice that we just access the latest segment commit info; no reader is opened on the segment in this phase.\

The `SampledClusterMetricsService` running on the persistent task node builds and stores a consolidated view of samples returned by nodes.
Regarding shard info metrics, `SampledClusterMetricsService` considers only the most up to date generations and handles corner cases such as indices being deleted or potentially even re-generated with the same name.

Like for RA-I, a separate service (`UsageReportCollector`) schedules a periodic function (with a default interval of 5 minutes) via a dedicated threadpool scheduler to collect metrics from all registered metrics providers; in the case of RA-S and IX, the sampled metrics provider exposed by `SampledClusterMetricsService` generates per-index billing usage records for both storage metrics by aggregating the most up-to-date per-shard info. Records for empty indices without any live documents are omitted. The sample records are then associated with the current reporting period (aka sampling period), which is "snapped" (or aligned) to multiples of the sampling period boundaries within the hour.

Because of its architecture/technology combination, the billing pipeline is not able to detect missing samples and is not able to adjust for them, resulting in potentially incorrect computations.
The billing pipeline operates using a "billing period" of 1h; the reporting (sampling) period must be set so the billing period is divisible by the sampling period, without remainder. In other words, there must be an integer number of sampling periods within a billing period. With the default sampling period of 5 minutes, this number is 12. The billing pipeline averages all samples in its billing period, but it only considers samples individually; therefore it performs a "live average": it divides each sample by the number of expected samples in the billing period to obtain a contribution to the final value and sums them.

For this reason, `UsageReportCollector` keeps a “cursor” in cluster state which refers to the last successfully transmitted sampling period. Upon successful publication, the cursor is updated with the last sampling timestamp. The persistent task node also retains in memory the most recently published sampling records.\
If `UsageReportCollector` detects that some previous sampling intervals have been missed (e.g. due to delays or failures due to unavailability of the usage API), it backfills sample records using linear interpolation between the latest successfully published samples and the most up-to-date samples.\
If there are no previous samples at all (e.g. after changing the persistent task node), we do a limited backfilling: only 2 additional timeframes, using constant interpolation. This covers a couple of common scenarios in which this happens (scaling and rolling upgrade).\
If no previous sample for a particular index exists, backfilling for that index is skipped (we just transmit the new available data); a missing sample for a single index means that either the index is new or a shard/node was not available.\
In any case, at most 24h of data is backfilled.

Besides VCU usage records, the same reporting infrastructure is used to report both RA-S and IX. However, for historic reasons, IX usage samples are reported per shard instead of per index.

Notes:

* Each node keeps a cache of the metering info to return to the persistent task node
    * When queried, the node recalculates only the information that needs to be updated, based on the generation.
    * If metering infos of a shard on a particular node haven’t changed, that information  is omitted from the response when being queried by the persistent task node (i.e. we only answer with the diff).
* `SampledClusterMetricsService` runs on every node of the cluster, but it effectively do something (generate and send records) only on the persistent task node; however, the persistent task can be re-allocated to a different node at any time (e.g. during a rolling upgrade), so any node needs to be ready to start collecting and sending usage metrics.
* UsageReportCollector also runs on every node; on search nodes, it will collect and forward metrics from the (single active) sampled storage metrics provider for RA-S/IX running on the persistent task node. On index nodes, it will collect and forward metrics from the ingest counter metrics provider for RA-I.

Code references:

* [`SampledClusterMetricsSchedulingTask`](https://github.com/elastic/elasticsearch-serverless/blob/main/modules/serverless-metering/src/main/java/co/elastic/elasticsearch/metering/sampling/SampledClusterMetricsSchedulingTask.java): periodically polls metering updates via SampledClusterMetricsService
* [`SampledClusterMetricsService`](https://github.com/elastic/elasticsearch-serverless/blob/main/modules/serverless-metering/src/main/java/co/elastic/elasticsearch/metering/sampling/SampledClusterMetricsSchedulingTask.java): maintains consolidated view of latest sampled metrics, such as shard or tier samples.
* [`SampledStorageMetricsProvider`](https://github.com/elastic/elasticsearch-serverless/blob/d7992ec3b4a7e140f8421509f4ed46c4605f9037/modules/serverless-metering/src/main/java/co/elastic/elasticsearch/metering/sampling/SampledStorageMetricsProvider.java): provides per index RA-S and per shard IX usage samples using above
* [`SampledVCUMetricsProvider`](https://github.com/elastic/elasticsearch-serverless/blob/d7992ec3b4a7e140f8421509f4ed46c4605f9037/modules/serverless-metering/src/main/java/co/elastic/elasticsearch/metering/sampling/SampledVCUMetricsProvider.java): provides per tier VCU samples
* [`UsageReportCollector`](https://github.com/elastic/elasticsearch-serverless/blob/main/modules/serverless-metering/src/main/java/co/elastic/elasticsearch/metering/usagereports/UsageReportCollector.java): periodically collects and publishes usage records from available sampled metrics providers, backfilling samples if necessary


## Metering stats API

Returns billing-related statistics for one or more indices. For data streams, the API retrieves statistics for the stream’s backing indices.

### Request

`GET _metering/stats/<target>`

`GET _metering/stats`

### Prerequisites

- This API is intended only for Solutions. The expectation is that no one else will use it, and it will not be exposed publicly.
- This API is an internal only API; it requires the primary authenticated user to be a serverless operator (e.g. the Kibana user).
- The API optionally supports secondary authentication. When the secondary authentication header is sent the secondary user's privileges will be used to do the work. If the secondary authentication header is not sent then work is done using the primary (and only) user's privileges. Note - the primary authentication must be a serverless operator since this API is internal only.

Secondary authentication is specified via the `es-secondary-authorization` header:
```shell
curl -s -u elastic-admin:elastic-password -H "es-secondary-authorization: Basic dXNlcjE6YWJjZGVmZw==" -X GET http://localhost:9200/_metering/stats
```

### Path parameters

`<target>`

(Optional, string) Comma-separated list of data streams, indices, and aliases used to limit the request. Supports wildcards (*). To target all data streams and indices, omit this parameter or use *

Examples: `/_metering/stats/foo*,bar*` or `/_metering/stats/foo1`

The API by default returns info for all indices and data stream. Note that "all indices" means "all the indices a user can see"; `<target>` is always filtered according to the secondary user's privileges).

### Response codes

- `200` OK
- `404` The index (or indices) specified in `<target>` do not exist.
Note that this also includes the case in which the index (or indices) cannot be accessed by the user (i.e. the secondary user's privilege is not sufficient to access the index (or indices)).


### Response body

The response body reports the per-index/per-datastream metering statistics of the matching indices.

- `_total`
(object) summary information
  - `num_docs` (long) number of live documents (documents minus the deleted ones)
  - `size_in_bytes` (long) the internal size (in bytes) for the index, as forwarded  to billing for billing calculations (*)(**)
- `indices` (array) per-index information. Each entry is an object with
  - `name` (string) the index name
  - `num_docs` (long) number of live documents (documents minus the deleted ones)
  - `size_in_bytes` (long) the internal size (in bytes) for the index, as forwarded  to billing for billing calculations (**)
  - `datastream` (string, optional) if this index backs a datastream, this field contains the datastream name

- `datastreams` (array, optional)
    - `name` (string) the datastream name
    - `num_docs` (long) number of live documents (documents minus the deleted ones)
    - `size_in_bytes` (long) the internal size (in bytes) for the data stream, as forwarded to billing for billing calculations (**)

All indices, including datastream indices, will appear in the indices section. Example:
```json
{
    "_total": {
        "num_docs": 8,
        "size_in_bytes": 26454
    },
    "indices": [
      {
          "name": ".ds-my-datastream-03-02-2024-00001",
          "num_docs": 6,
          "size_in_bytes": 15785,
          "datastream": "my-datastream"
      },
      {
        "name": "my-index-000001",
        "num_docs": 2,
        "size_in_bytes": 11462
      }
    ],
    "datastreams": [
      {
        "name": "my-datastream",
        "num_docs": 6,
        "size_in_bytes": 15785
      }
    ]
}
```
(*) note that billing filters out some indices, (a predefined set of `.` and system indices) before computing the final bill, so there will not be a perfect match here.

(**) currently, the API choose the metric to use for `size_in_bytes` based on the project type (e.g. IX for general purpose, RA-S for security and observability). This is likely a temporary solution, until a public API is available.

