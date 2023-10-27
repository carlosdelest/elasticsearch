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

package co.elastic.elasticsearch.serverless.transform;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.transform.Transform;
import org.elasticsearch.xpack.transform.TransformExtension;

import java.util.Collection;
import java.util.List;

public class ServerlessTransformPlugin extends Transform {

    public static final String NAME = "serverless-transform";

    public final SetOnce<List<ActionFilter>> actionFilters = new SetOnce<>();

    private final TransformExtension transformExtension = new ServerlessTransformExtension();

    public ServerlessTransformPlugin(Settings settings) {
        super(settings);
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        ThreadContext threadContext = services.threadPool().getThreadContext();
        actionFilters.set(List.of(new GetTransformStatsResponseFilter(threadContext)));
        return super.createComponents(services);
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        return actionFilters.get();
    }

    @Override
    public TransformExtension getTransformExtension() {
        return transformExtension;
    }
}
