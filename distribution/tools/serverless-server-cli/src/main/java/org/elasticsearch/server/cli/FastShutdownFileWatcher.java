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

package org.elasticsearch.server.cli;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.file.AbstractFileWatchingService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Supplier;

class FastShutdownFileWatcher extends AbstractFileWatchingService {

    private final Terminal terminal;
    private final Supplier<ServerProcess> server;

    FastShutdownFileWatcher(Terminal terminal, Path fastShutdownMarkerFile, Supplier<ServerProcess> server) {
        super(fastShutdownMarkerFile);
        this.terminal = terminal;
        this.server = server;
    }

    @Override
    protected void processFileChanges() throws IOException {
        ServerProcess process = server.get();
        if (process != null) {
            terminal.println("Fast shutdown marker detected. Killing Elasticsearch...");
            process.forceStop();
        }
    }

    @Override
    protected void processInitialFileMissing() {}
}
