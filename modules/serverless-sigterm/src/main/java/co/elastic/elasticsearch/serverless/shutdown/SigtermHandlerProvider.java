/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.serverless.shutdown;

import org.elasticsearch.node.internal.TerminationHandler;
import org.elasticsearch.node.internal.TerminationHandlerProvider;

/**
 * This is the class that's actually injected via SPI. Plumbing.
 */
public class SigtermHandlerProvider implements TerminationHandlerProvider {
    private final ServerlessSigtermPlugin plugin;

    public SigtermHandlerProvider() {
        throw new IllegalStateException(this.getClass().getSimpleName() + " must be constructed using PluginsService");
    }

    public SigtermHandlerProvider(ServerlessSigtermPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public TerminationHandler handler() {
        return this.plugin.getTerminationHandler();
    }
}
