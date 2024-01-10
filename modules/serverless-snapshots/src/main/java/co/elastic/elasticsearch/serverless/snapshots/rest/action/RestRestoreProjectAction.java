/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.serverless.snapshots.rest.action;

import co.elastic.elasticsearch.serverless.snapshots.action.TransportRestoreProjectAction;

import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.INTERNAL)
public class RestRestoreProjectAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_snapshot/{repository}/{snapshot}/_restore_project"));
    }

    @Override
    public String getName() {
        return "restore_project_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String repository = request.param("repository");
        String snapshot = request.param("snapshot");

        ensureNoWildcardsOrCommas("repository", repository);
        ensureNoWildcardsOrCommas("snapshot", snapshot);

        // at the rest layer we receive a repository and a snapshot (*only!*), translate that into a RestoreSnapshotRequest for
        // the transport layer, setting the options we actually want. specifically, this is always a full cluster restore.
        RestoreSnapshotRequest restoreSnapshotRequest = new RestoreSnapshotRequest(repository, snapshot);
        // the default for RestoreSnapshotRequest is that all indices are restored, and we change nothing here to alter that
        restoreSnapshotRequest.includeGlobalState(true); // always include the global state
        restoreSnapshotRequest.masterNodeTimeout(TimeValue.MAX_VALUE); // infinite timeout
        restoreSnapshotRequest.waitForCompletion(true); // you must wait for completion, there's no option no to
        restoreSnapshotRequest.partial(true); // it is permissible to restore partial snapshots
        return channel -> client.execute(TransportRestoreProjectAction.TYPE, restoreSnapshotRequest, new RestToXContentListener<>(channel));
    }

    private static void ensureNoWildcardsOrCommas(String context, String s) {
        if (s.indexOf('*') != -1 || s.indexOf(',') != -1) {
            final var error = Strings.format("Forbidden character(s) in %s [%s], wildcards and commas are not allowed", context, s);
            throw new IllegalArgumentException(error);
        }
    }
}
