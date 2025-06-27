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

package co.elastic.elasticsearch.serverless.observability.api;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;

public class LogsEssentialsAsyncSearchRequestValidator extends AbstractLogsEssentialsRequestValidator {
    public LogsEssentialsAsyncSearchRequestValidator(Settings settings) {
        super(settings);
    }

    @Override
    public String actionName() {
        return SubmitAsyncSearchAction.NAME;
    }

    @Override
    protected SearchSourceBuilder getSource(ActionRequest request) {
        assert request instanceof SubmitAsyncSearchRequest;
        return ((SubmitAsyncSearchRequest) request).getSearchRequest().source();
    }
}
