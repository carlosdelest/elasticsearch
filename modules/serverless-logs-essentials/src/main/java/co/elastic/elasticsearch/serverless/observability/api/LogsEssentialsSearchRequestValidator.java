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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class LogsEssentialsSearchRequestValidator extends AbstractLogsEssentialsRequestValidator {
    public LogsEssentialsSearchRequestValidator(Settings settings) {
        super(settings);
    }

    @Override
    public String actionName() {
        return TransportSearchAction.NAME;
    }

    @Override
    protected SearchSourceBuilder getSource(ActionRequest request) {
        assert request instanceof SearchRequest;
        return ((SearchRequest) request).source();
    }
}
