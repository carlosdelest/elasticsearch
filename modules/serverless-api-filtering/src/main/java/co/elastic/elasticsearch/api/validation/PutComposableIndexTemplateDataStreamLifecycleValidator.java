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

import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.List;

public class PutComposableIndexTemplateDataStreamLifecycleValidator extends DataStreamLifecycleValidator<
    TransportPutComposableIndexTemplateAction.Request> {
    public PutComposableIndexTemplateDataStreamLifecycleValidator(ThreadContext threadContext) {
        super(threadContext);
    }

    @Override
    protected List<DataStreamLifecycle> getLifecyclesFromRequest(TransportPutComposableIndexTemplateAction.Request request) {
        return getLifecyclesFromTemplate(request.indexTemplate().template());
    }

    @Override
    public String actionName() {
        return TransportPutComposableIndexTemplateAction.TYPE.name();
    }
}
