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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.function.Supplier;
import java.util.stream.Stream;

class FastShutdownFileWatcher extends AbstractFileWatchingService {

    private final Path file;
    private final Terminal terminal;
    private final Supplier<ServerProcess> server;
    private volatile boolean shutdownStarted = false;

    FastShutdownFileWatcher(Terminal terminal, Path fastShutdownMarkerFile, Supplier<ServerProcess> server) {
        super(fastShutdownMarkerFile.getParent());
        this.file = fastShutdownMarkerFile;
        this.terminal = terminal;
        this.server = server;
    }

    // true if the fast shutdown file was detected and the server will be stopped
    boolean isStopping() {
        return shutdownStarted;
    }

    @Override
    protected void processFileChanges(Path file) throws IOException {
        if (file.equals(this.file) == false) return;    // not our file

        ServerProcess process = server.get();
        if (process != null) {
            terminal.println("Fast shutdown marker detected. Killing Elasticsearch...");
            shutdownStarted = true;
            process.forceStop();
        }
    }

    @Override
    protected void processInitialFilesMissing() {}

    // the following methods are a workaround to ensure exclusive access for files
    // required by child watchers; this is required because we only check the caller's module
    // not the entire stack
    @Override
    protected boolean filesExists(Path path) {
        return Files.exists(path);
    }

    @Override
    protected boolean filesIsDirectory(Path path) {
        return Files.isDirectory(path);
    }

    @Override
    protected <A extends BasicFileAttributes> A filesReadAttributes(Path path, Class<A> clazz) throws IOException {
        return Files.readAttributes(path, clazz);
    }

    @Override
    protected Stream<Path> filesList(Path dir) throws IOException {
        return Files.list(dir);
    }

    @Override
    protected Path filesSetLastModifiedTime(Path path, FileTime time) throws IOException {
        return Files.setLastModifiedTime(path, time);
    }

    @Override
    protected InputStream filesNewInputStream(Path path) throws IOException {
        return Files.newInputStream(path);
    }
}
