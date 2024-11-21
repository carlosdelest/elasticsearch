Versioning Serverless Elasticsearch
========================

This is a counterpart to `VERSIONING.md` present in the main Elasticsearch codebase.
Serverless-specific modifications to that doc are present here.

## Transport protocol

The `SS` part of transport version numbers are for Serverless-specific constants.
These are present in the `ServerlessTransportVersions` class.

When a serialization change is needed in the Serverless repo, a new constant
should be added to that class that take the id of the latest transport version
used in the currently referenced commit of the Elasticsearch submodule,
and increments the `SS` part of the id.

For example, if the current commit of the elasticsearch repo referenced by Serverless
has a highest version id of 8_801_00_0 (which may be less than the highest
transport version at the head of elasticsearch main),
the new Serverless-only transport version constant should be 8_801_01_0.

Once all deployed Serverless clusters have moved past that specific version
onto a new transport version (either from the elasticsearch repo,
or a different Serverless-specific transport version), the constant
and all associated checks can be removed from the Serverless codebase.

### Managing patches and backports

When backporting a change for Serverless, **great care** must be taken to ensure
you don’t get two conflicting transport version changes. If an emergency patch
is produced with a transport change, branched off a commit that is not current
head of Serverless main, then the Serverless clusters next need to be promoted
to a version that knows what the backported transport version id is.
The cluster **must not** go from a commit with the backport,
to a commit without the backport, else you could get a serialization mismatch and node crashes.

## Index version

Although the index version id has got an `SS` component like transport version,
we have not had to produce a Serverless-specific index format, and there is
no infrastructure to support such divergence. If needed, it would work
in a similar way to transport versions.
