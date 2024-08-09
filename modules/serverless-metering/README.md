
# Metering plugin

This plugin adds functionalities to serverless Elasticsearch Metering for billing purposes.

### Metering stats API

Returns billing-related statistics for one or more indices. For data streams, the API retrieves statistics for the stream’s backing indices.

#### Request

`GET _metering/stats/<target>`

`GET _metering/stats`

#### Prerequisites

- This API is intended only for Solutions. The expectation is that no one else will use it, and it will not be exposed publicly.
- This API is an internal only API; it requires the primary authenticated user to be a serverless operator (e.g. the Kibana user).
- The API optionally supports secondary authentication. When the secondary authentication header is sent the secondary user's privileges will be used to do the work. If the secondary authentication header is not sent then work is done using the primary (and only) user's privileges. Note - the primary authentication must be a serverless operator since this API is internal only.

Secondary authentication is specified via the `es-secondary-authorization` header:
```shell
curl -s -u elastic-admin:elastic-password -H "es-secondary-authorization: Basic dXNlcjE6YWJjZGVmZw==" -X GET http://localhost:9200/_metering/stats
```

#### Path parameters

`<target>`

(Optional, string) Comma-separated list of data streams, indices, and aliases used to limit the request. Supports wildcards (*). To target all data streams and indices, omit this parameter or use *

Examples: `/_metering/stats/foo*,bar*` or `/_metering/stats/foo1`

The API by default returns info for all indices and data stream. Note that "all indices" means "all the indices a user can see"; `<target>` is always filtered according to the secondary user's privileges).

#### Response codes

- `200` OK
- `404` The index (or indices) specified in `<target>` do not exist.
Note that this also includes the case in which the index (or indices) cannot be accessed by the user (i.e. the secondary user's privilege is not sufficient to access the index (or indices)).


#### Response body

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

