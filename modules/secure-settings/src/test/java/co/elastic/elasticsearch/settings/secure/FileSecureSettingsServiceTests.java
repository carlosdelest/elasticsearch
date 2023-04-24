/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.settings.secure;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.nio.file.Path;

import static org.mockito.Mockito.mock;

public class FileSecureSettingsServiceTests extends ESTestCase {

    private FileSecureSettingsService fileSecureSettingsService;
    private Environment env;
    private ClusterService clusterService;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        env = newEnvironment();
        clusterService = mock(ClusterService.class);
        fileSecureSettingsService = new FileSecureSettingsService(clusterService, env);
    }

    public void testDirectoryName() {
        Path secureSettingsDir = fileSecureSettingsService.watchedFileDir();
        assertTrue(secureSettingsDir.startsWith(env.configFile()));
        assertTrue(secureSettingsDir.endsWith("secrets"));
    }

    public void testFileName() {
        Path secureSettingsFile = fileSecureSettingsService.watchedFile();
        assertTrue(secureSettingsFile.startsWith(env.configFile()));
        assertTrue(secureSettingsFile.endsWith("secrets.json"));
    }
}
