/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.common.settings.LocallyMountedSecrets;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import static org.hamcrest.Matchers.instanceOf;

public class LocallyMountedSecretsLoaderTests extends ESTestCase {

    private Environment env;

    @Before
    public void setup() {
        env = newEnvironment();
    }

    public void testCreation() throws Exception {
        var loaderInstance = new LocallyMountedSecretsLoader();
        try (var loadedSecrets = loaderInstance.load(env, MockTerminal.create())) {
            assertThat(loadedSecrets.secrets(), instanceOf(LocallyMountedSecrets.class));
            assertTrue(loadedSecrets.password().isEmpty());
            assertFalse(loaderInstance.supportsSecurityAutoConfiguration());
        }
    }

    public void testBootstrapNotSupported() {
        var loaderInstance = new LocallyMountedSecretsLoader();
        assertEquals(
            "Bootstrapping locally mounted secrets is not supported",
            expectThrows(IllegalArgumentException.class, () -> loaderInstance.bootstrap(env, new SecureString(new char[0]))).getMessage()
        );
    }
}
