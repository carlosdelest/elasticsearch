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

package co.elastic.elasticsearch.ml.serverless.actionfilters;

import co.elastic.elasticsearch.ml.serverless.ServerlessMachineLearningExtension;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.api.filtering.ApiFilteringActionFilter;
import org.elasticsearch.xpack.core.ml.action.UpgradeJobModelSnapshotAction;

public class UpgradeJobModelSnapshotResponseFilter extends ApiFilteringActionFilter<UpgradeJobModelSnapshotAction.Response> {

    public UpgradeJobModelSnapshotResponseFilter(ThreadContext threadContext) {
        super(threadContext, UpgradeJobModelSnapshotAction.NAME, UpgradeJobModelSnapshotAction.Response.class);
    }

    /**
     * This method replaces a non-empty node ID in the response with the literal string
     * "serverless" for non-operator users.
     */
    @Override
    protected UpgradeJobModelSnapshotAction.Response filterResponse(UpgradeJobModelSnapshotAction.Response response) {
        if (Strings.isNullOrEmpty(response.getNode())) {
            return response;
        } else {
            return new UpgradeJobModelSnapshotAction.Response(
                response.isCompleted(),
                ServerlessMachineLearningExtension.SERVERLESS_VIRTUAL_ML_NODE.getId()
            );
        }
    }
}
