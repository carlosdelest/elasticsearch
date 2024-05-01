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

package co.elastic.elasticsearch.api.validation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.tasks.Task;

public class IndicesAliasRequestValidator implements MappedActionFilter {
    @Override
    public int order() {
        return 0;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        assert request instanceof IndicesAliasesRequest;
        var aliasRequest = (IndicesAliasesRequest) request;
        if (aliasRequest.getAliasActions()
            .stream()
            .anyMatch(
                aliasAction -> aliasAction.routing() != null || aliasAction.indexRouting() != null || aliasAction.searchRouting() != null
            )) {
            throw new IllegalArgumentException("Routing values are not supported in serverless mode");
        }
        chain.proceed(task, action, request, listener);
    }

    @Override
    public String actionName() {
        return TransportIndicesAliasesAction.NAME;
    }
}
