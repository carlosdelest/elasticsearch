/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.serverless.transform;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.transform.Transform;

public class ServerlessTransformTests extends ESTestCase {
    public void testIncludeNodeInfo() {
        Transform serverlesstransform = new ServerlessTransformPlugin(Settings.EMPTY);
        assertFalse(serverlesstransform.includeNodeInfo());
    }
}
