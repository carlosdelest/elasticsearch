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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.env.Environment;
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.reservedstate.service.ReservedClusterStateService;
import org.elasticsearch.reservedstate.service.ReservedStateChunk;
import org.elasticsearch.reservedstate.service.ReservedStateVersionCheck;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.reservedstate.service.ReservedStateVersionCheck.HIGHER_OR_SAME_VERSION;
import static org.elasticsearch.reservedstate.service.ReservedStateVersionCheck.HIGHER_VERSION_ONLY;
import static org.elasticsearch.xcontent.XContentType.JSON;

/**
 * Extends {@link FileSettingsService} to watch and read additional project files alongside the main settings file
 */
public class MultiProjectFileSettingsService extends FileSettingsService {

    private static final Logger logger = LogManager.getLogger(MultiProjectFileSettingsService.class);
    private static final String PROJECT_FILE_PREFIX = "project-";
    private static final String PROJECT_FILE_SUFFIX = ".json";
    private static final String SECRETS_FILE_SUFFIX = ".secrets.json";
    private static final String PROJECTS_KEY = "projects";

    private final Set<ProjectId> registeredProjects = new HashSet<>();
    private final Map<ProjectId, Path> projectFiles = new HashMap<>();
    private final Map<ProjectId, Path> secretsFiles = new HashMap<>();
    private final ReservedClusterStateService stateService;

    public MultiProjectFileSettingsService(
        ClusterService clusterService,
        ReservedClusterStateService stateService,
        Environment environment,
        FileSettingsHealthTracker healthTracker
    ) {
        super(clusterService, stateService, environment, healthTracker);
        this.stateService = stateService;
    }

    @Override
    protected Logger logger() {
        return logger;
    }

    @Override
    protected XContentParser createParser(InputStream stream) throws IOException {
        // exclude the projects key from data passed to the reserved state service
        return JSON.xContent()
            .createParser(XContentParserConfiguration.EMPTY.withFiltering(null, null, Set.of(PROJECTS_KEY), false), stream);
    }

    private ProjectId tryParseProjectSecretsFileName(String fileName) {
        if (fileName.startsWith(PROJECT_FILE_PREFIX) && fileName.endsWith(SECRETS_FILE_SUFFIX)) {
            return ProjectId.fromId(fileName.substring(PROJECT_FILE_PREFIX.length(), fileName.length() - SECRETS_FILE_SUFFIX.length()));
        }
        return null;
    }

    private ProjectId tryParseProjectFileName(String fileName) {
        if (fileName.startsWith(PROJECT_FILE_PREFIX) && fileName.endsWith(PROJECT_FILE_SUFFIX)) {
            return ProjectId.fromId(fileName.substring(PROJECT_FILE_PREFIX.length(), fileName.length() - PROJECT_FILE_SUFFIX.length()));
        }
        return null;
    }

    @Override
    protected void processFile(Path file, boolean startup) throws ExecutionException, InterruptedException, IOException {
        ReservedStateVersionCheck versionCheck = startup ? HIGHER_OR_SAME_VERSION : HIGHER_VERSION_ONLY;
        ProjectId projectId;

        if ((projectId = tryParseProjectSecretsFileName(file.getFileName().toString())) != null) {
            logger().debug("Recording new secrets file [{}] for project [{}]{}", file, projectId, startup ? " on service start" : "");
            secretsFiles.putIfAbsent(projectId, file);
            checkProcessProjectFiles(projectId, versionCheck);
        } else if ((projectId = tryParseProjectFileName(file.getFileName().toString())) != null) {
            logger().debug("Recording new project file [{}] for project [{}]{}", file, projectId, startup ? " on service start" : "");
            projectFiles.putIfAbsent(projectId, file);
            checkProcessProjectFiles(projectId, versionCheck);
        } else if (watchedFile().equals(file)) {
            // main settings file - check the list of projects
            Set<ProjectId> newProjects = readNewRegisteredProjects(file);

            // process main settings file first
            // TODO: do we need to combine this update with adding/updating projects below so it's atomic?
            super.processFile(file, startup);

            // then create all the new projects
            // TODO: also remove projects
            ExecutionException error = null;
            for (ProjectId p : newProjects) {
                // process each one individually, error in one shouldn't affect the others
                logger().debug("Registering new project [{}]{}", p, startup ? " on service start" : "");
                registeredProjects.add(p);
                try {
                    checkProcessProjectFiles(p, versionCheck);
                } catch (ExecutionException e) {
                    error = ExceptionsHelper.useOrSuppress(error, e);
                }
            }
            if (error != null) {
                throw error;
            }
        } else {
            logger().debug("Received notification for unknown file {}", file);
        }
    }

    @Override   // visible for testing
    protected void processInitialFilesMissing() throws ExecutionException, InterruptedException {
        super.processInitialFilesMissing();
    }

    @Override  // visible for testing
    protected void onProcessFileChangesException(Path file, Exception e) {
        super.onProcessFileChangesException(file, e);
    }

    @FixForMultiProject // handle project removals
    private Set<ProjectId> readNewRegisteredProjects(Path settingsFile) throws IOException {
        try (var bis = new BufferedInputStream(Files.newInputStream(settingsFile))) {
            Map<String, Object> data = XContentHelper.convertToMap(JSON.xContent(), bis, false, Set.of(PROJECTS_KEY), Set.of());
            return ((Collection<?>) data.getOrDefault(PROJECTS_KEY, List.of())).stream().map(o -> {
                if (o instanceof String s) return s;
                throw new XContentParseException("Project key [" + o + "] is not a String");
            }).map(ProjectId::fromId).filter(s -> registeredProjects.contains(s) == false).collect(Collectors.toSet());
        } catch (Exception e) {
            logger().error("Could not read list of projects from settings file", e);
            throw e;
        }
    }

    private void checkProcessProjectFiles(ProjectId projectId, ReservedStateVersionCheck versionCheck) throws InterruptedException,
        ExecutionException {
        // only process a project if it's in the list of registered projects, has a project settings file (even if its empty) and a
        // secrets file
        Path projectFile = projectFiles.get(projectId);
        Path secretsFile = secretsFiles.get(projectId);
        if (registeredProjects.contains(projectId) && projectFile != null && secretsFile != null) {
            logger.info("Processing changes for project [{}] with settings file [{}]", projectId, projectFile);
            processReservedClusterStateFiles(projectId, versionCheck, projectFile, secretsFile);
        } else {
            logger.debug(
                "Partial project configuration detected for project: [{}] with project in settings: [{}] project "
                    + "file: [{}] secrets file [{}]",
                projectId.id(),
                registeredProjects.contains(projectId),
                projectFile,
                secretsFile
            );
        }
    }

    private void processReservedClusterStateFiles(ProjectId projectId, ReservedStateVersionCheck versionCheck, Path... files)
        throws InterruptedException, ExecutionException {
        PlainActionFuture<Void> completion = new PlainActionFuture<>();

        List<ReservedStateChunk> chunks = new ArrayList<>(files.length);
        for (var file : files) {
            try (
                var stream = new BufferedInputStream(filesNewInputStream(file));
                var parser = JSON.xContent().createParser(XContentParserConfiguration.EMPTY, stream)
            ) {
                chunks.add(stateService.parse(projectId, NAMESPACE, parser));
            } catch (Exception exception) {
                throw new ExecutionException("Error processing project: [" + projectId.id() + "]", exception);
            }
        }

        // TODO: what needs doing with health reporting?
        stateService.process(projectId, NAMESPACE, chunks, versionCheck, e -> completeProcessing(e, completion));
        // TODO: parallelise somehow
        completion.get();
    }

    @Override
    public void handleSnapshotRestore(ClusterState clusterState, Metadata.Builder mdBuilder) {
        assert clusterState.metadata().projects().size() == 1
            : "Cluster snapshot restore not supported for multiple projects " + clusterState.metadata().projects().keySet();
        super.handleSnapshotRestore(clusterState, mdBuilder);
    }

    @Override
    protected boolean filesExists(Path path) {
        return Files.exists(path);
    }

    @Override
    protected boolean filesIsDirectory(Path path) {
        return Files.isDirectory(path);
    }

    @Override
    protected boolean filesIsSymbolicLink(Path path) {
        return Files.isSymbolicLink(path);
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
