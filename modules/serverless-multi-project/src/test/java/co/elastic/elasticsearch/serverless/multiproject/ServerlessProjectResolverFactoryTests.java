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

package co.elastic.elasticsearch.serverless.multiproject;

import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class ServerlessProjectResolverFactoryTests extends ESTestCase {

    public void testCreateResolverWhenEnabled() {
        Settings settings = Settings.builder().put(ServerlessMultiProjectPlugin.MULTI_PROJECT_ENABLED.getKey(), true).build();
        final ServerlessProjectResolverFactory factory = new ServerlessProjectResolverFactory(new ServerlessMultiProjectPlugin(settings));
        assertThat(factory.create(), instanceOf(ServerlessProjectResolver.class));
    }

    public void testCreateResolverWhenDisabled() {
        Settings settings = randomBoolean()
            ? Settings.EMPTY
            : Settings.builder().put(ServerlessMultiProjectPlugin.MULTI_PROJECT_ENABLED.getKey(), false).build();
        final ServerlessProjectResolverFactory factory = new ServerlessProjectResolverFactory(new ServerlessMultiProjectPlugin(settings));
        assertThat(factory.create(), sameInstance(DefaultProjectResolver.INSTANCE));
    }

}
