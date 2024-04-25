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
import org.elasticsearch.action.admin.indices.template.post.SimulateIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.post.SimulateIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.post.SimulateTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.lifecycle.PutDataStreamLifecycleAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;

import java.util.Map;
import java.util.function.Function;

/**
 * An action filter that performs a validation of incoming requests with a data stream lifecycle object.
 * The validation is preventing non-operator users to disable the lifecycle of a data stream.
 */
public class DataStreamLifecycleValidationActionFilter implements ActionFilter {
    private final DataStreamLifecycleValidator validator;

    private final Map<String, Function<? extends ActionRequest, DataStreamLifecycle>> mappingFunctions = Map.of(
        PutDataStreamLifecycleAction.INSTANCE.name(),
        (PutDataStreamLifecycleAction.Request request) -> request.getLifecycle(),

        PutComponentTemplateAction.NAME,
        (PutComponentTemplateAction.Request request) -> fromTemplate(request.componentTemplate().template()),

        TransportPutComposableIndexTemplateAction.TYPE.name(),
        (TransportPutComposableIndexTemplateAction.Request request) -> fromIndexTemplateRequest(request),

        SimulateTemplateAction.NAME,
        (SimulateTemplateAction.Request request) -> fromIndexTemplateRequest(request.getIndexTemplateRequest()),

        SimulateIndexTemplateAction.NAME,
        (SimulateIndexTemplateRequest request) -> fromIndexTemplateRequest(request.getIndexTemplateRequest())
    );

    @Nullable
    private DataStreamLifecycle fromTemplate(@Nullable Template template) {
        return template == null ? null : template.lifecycle();
    }

    @Nullable
    private DataStreamLifecycle fromIndexTemplateRequest(@Nullable TransportPutComposableIndexTemplateAction.Request request) {
        return request == null ? null : fromTemplate(request.indexTemplate().template());
    }

    public DataStreamLifecycleValidationActionFilter(ThreadContext threadContext) {
        this.validator = new DataStreamLifecycleValidator(threadContext);
    }

    @Override
    public int order() {
        return 0;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        if (mappingFunctions.containsKey(action)) {
            Function<Request, DataStreamLifecycle> extractLifecycleFunction = (Function<Request, DataStreamLifecycle>) mappingFunctions.get(
                action
            );
            DataStreamLifecycle lifecycle = extractLifecycleFunction.apply(request);
            validator.validateLifecycle(lifecycle);
        }

        chain.proceed(task, action, request, listener);
    }

}
