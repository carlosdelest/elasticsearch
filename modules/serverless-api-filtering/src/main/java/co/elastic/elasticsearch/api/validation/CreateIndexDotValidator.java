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

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.Set;

public class CreateIndexDotValidator extends DotPrefixValidator<CreateIndexRequest> {
    public CreateIndexDotValidator(ThreadContext threadContext, ClusterService clusterService) {
        super(threadContext, clusterService);
    }

    @Override
    protected Set<String> getIndicesFromRequest(CreateIndexRequest request) {
        return Set.of(request.index());
    }

    @Override
    public String actionName() {
        return TransportCreateIndexAction.TYPE.name();
    }
}
