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
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;

public class PutComposableTemplateSettingsValidator extends PublicSettingsValidator<TransportPutComposableIndexTemplateAction.Request> {

    public PutComposableTemplateSettingsValidator(ThreadContext threadContext, IndexScopedSettings indexScopedSettings) {
        super(threadContext, indexScopedSettings);
    }

    @Override
    public String actionName() {
        return TransportPutComposableIndexTemplateAction.TYPE.name();
    }

    @Override
    protected Settings getSettingsFromRequest(TransportPutComposableIndexTemplateAction.Request r) {
        if (r.indexTemplate() != null) {
            if (r.indexTemplate().template() != null) {
                return r.indexTemplate().template().settings();
            }
        }
        return null;
    }
}
