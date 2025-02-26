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

package co.elastic.elasticsearch.serverless.multiproject.reservedstate.ingest;

import co.elastic.elasticsearch.serverless.multiproject.ServerlessMultiProjectPlugin;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineTransportAction;
import org.elasticsearch.action.ingest.ReservedPipelineAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ReservedStateErrorMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateHandlerMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.IngestTestPlugin;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.elasticsearch.common.bytes.BytesReference.bytes;
import static org.elasticsearch.ingest.IngestPipelineTestUtils.putJsonPipelineRequest;
import static org.elasticsearch.xcontent.XContentType.JSON;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class MultiProjectIngestFileSettingsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ServerlessMultiProjectPlugin.class, CustomIngestTestPlugin.class);
    }

    @Override
    protected boolean multiProjectIntegrationTest() {
        return true;
    }

    @Override
    public Settings buildEnvSettings(Settings settings) {
        return Settings.builder()
            .put(super.buildEnvSettings(settings))
            .put(ServerlessMultiProjectPlugin.MULTI_PROJECT_ENABLED.getKey(), true)
            .build();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ServerlessMultiProjectPlugin.MULTI_PROJECT_ENABLED.getKey(), true)
            .build();
    }

    private static AtomicLong versionCounter = new AtomicLong(1);

    private static final String settingsJSON = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "projects": ["%s"],
             "state": {}
        }""";

    private static String projectJSON = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "ingest_pipelines": {
                   "my_ingest_pipeline": {
                       "description": "_description",
                       "processors": [
                          {
                            "test" : {
                              "field": "pipeline",
                              "value": "pipeline"
                            }
                          }
                       ]
                   },
                   "my_ingest_pipeline_1": {
                       "description": "_description",
                       "processors": [
                          {
                            "test" : {
                              "field": "pipeline",
                              "value": "pipeline"
                            }
                          }
                       ]
                   }
                 }
             }
        }""";

    private static String projectSecretsJSON = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {}
        }""";

    private static final String projectErrorJSON = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "ingest_pipelines": {
                   "my_ingest_pipeline": {
                       "description": "_description",
                       "processors":
                          {
                            "foo" : {
                              "field": "pipeline",
                              "value": "pipeline"
                            }
                          }
                       ]
                   }
                 }
             }
        }""";

    private final ProjectId projectId = randomUniqueProjectId();

    private void writeProjectJSONFiles(String node, String json) throws Exception {
        FileSettingsService fileSettingsService = internalCluster().getInstance(FileSettingsService.class, node);
        assertTrue(fileSettingsService.watching());

        Files.deleteIfExists(fileSettingsService.watchedFile());

        Files.createDirectories(fileSettingsService.watchedFileDir());
        Path tempFilePath = createTempFile();

        String settings = Strings.format(settingsJSON, versionCounter.incrementAndGet(), projectId.id());
        logger.info("--> writing main settings config to node {} with path {}", node, tempFilePath);
        logger.info(settings);
        Files.write(tempFilePath, settings.getBytes(StandardCharsets.UTF_8));
        Files.move(tempFilePath, fileSettingsService.watchedFile(), StandardCopyOption.ATOMIC_MOVE);

        String project = Strings.format(json, versionCounter.incrementAndGet());
        tempFilePath = createTempFile();
        logger.info("--> writing project settings config to node {} with path {}", node, tempFilePath);
        logger.info(project);
        Files.write(tempFilePath, project.getBytes(StandardCharsets.UTF_8));
        Files.move(
            tempFilePath,
            fileSettingsService.watchedFile().resolveSibling("project-" + projectId.id() + ".json"),
            StandardCopyOption.ATOMIC_MOVE
        );

        String secret = Strings.format(projectSecretsJSON, versionCounter.incrementAndGet());
        tempFilePath = createTempFile();
        logger.info("--> writing project secrets config to node {} with path {}", node, tempFilePath);
        logger.info(secret);
        Files.write(tempFilePath, secret.getBytes(StandardCharsets.UTF_8));
        Files.move(
            tempFilePath,
            fileSettingsService.watchedFile().resolveSibling("project-" + projectId.id() + ".secrets.json"),
            StandardCopyOption.ATOMIC_MOVE
        );
    }

    private Tuple<CountDownLatch, AtomicLong> setupClusterStateListener(String node) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch savedClusterState = new CountDownLatch(1);
        AtomicLong metadataVersion = new AtomicLong(-1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                if (event.state().metadata().hasProject(projectId) == false) {
                    return;
                }
                ReservedStateMetadata reservedState = event.state()
                    .metadata()
                    .getProject(projectId)
                    .reservedStateMetadata()
                    .get(FileSettingsService.NAMESPACE);
                if (reservedState != null) {
                    ReservedStateHandlerMetadata handlerMetadata = reservedState.handlers().get(ReservedPipelineAction.NAME);
                    if (handlerMetadata != null && handlerMetadata.keys().contains("my_ingest_pipeline")) {
                        clusterService.removeListener(this);
                        metadataVersion.set(event.state().metadata().version());
                        savedClusterState.countDown();
                    }
                }
            }
        });

        return new Tuple<>(savedClusterState, metadataVersion);
    }

    private void assertPipelinesSaveOK(CountDownLatch savedClusterState, AtomicLong metadataVersion) throws Exception {
        boolean awaitSuccessful = savedClusterState.await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        final ClusterStateResponse clusterStateResponse = clusterAdmin().state(
            new ClusterStateRequest(TEST_REQUEST_TIMEOUT).waitForMetadataVersion(metadataVersion.get())
        ).get();

        ReservedStateMetadata reservedState = clusterStateResponse.getState()
            .metadata()
            .getProject(projectId)
            .reservedStateMetadata()
            .get(FileSettingsService.NAMESPACE);

        ReservedStateHandlerMetadata handlerMetadata = reservedState.handlers().get(ReservedPipelineAction.NAME);

        assertThat(handlerMetadata.keys(), allOf(notNullValue(), containsInAnyOrder("my_ingest_pipeline", "my_ingest_pipeline_1")));

        // Try using the REST API to update the my_autoscaling_policy policy
        // This should fail, we have reserved certain autoscaling policies in operator mode
        assertEquals(
            "Failed to process request [org.elasticsearch.action.ingest.PutPipelineRequest/unset] with errors: "
                + "[[my_ingest_pipeline] set as read-only by [file_settings]]",
            expectThrows(
                IllegalArgumentException.class,
                client().filterWithHeader(Map.of(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId.id()))
                    .execute(PutPipelineTransportAction.TYPE, sampleRestRequest("my_ingest_pipeline"))
            ).getMessage()
        );
    }

    public void testPoliciesApplied() throws Exception {
        ensureGreen();

        var savedClusterState = setupClusterStateListener(internalCluster().getMasterName());
        writeProjectJSONFiles(internalCluster().getMasterName(), projectJSON);

        assertPipelinesSaveOK(savedClusterState.v1(), savedClusterState.v2());
    }

    private Tuple<CountDownLatch, AtomicLong> setupClusterStateListenerForError(String node) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch savedClusterState = new CountDownLatch(1);
        AtomicLong metadataVersion = new AtomicLong(-1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                if (event.state().metadata().hasProject(projectId) == false) {
                    return;
                }
                ReservedStateMetadata reservedState = event.state()
                    .metadata()
                    .getProject(projectId)
                    .reservedStateMetadata()
                    .get(FileSettingsService.NAMESPACE);
                if (reservedState != null && reservedState.errorMetadata() != null) {
                    clusterService.removeListener(this);
                    metadataVersion.set(event.state().metadata().version());
                    savedClusterState.countDown();
                    assertEquals(ReservedStateErrorMetadata.ErrorKind.PARSING, reservedState.errorMetadata().errorKind());
                    assertThat(reservedState.errorMetadata().errors(), allOf(notNullValue(), hasSize(1)));
                    assertThat(
                        reservedState.errorMetadata().errors().get(0),
                        containsString("org.elasticsearch.xcontent.XContentParseException: [17:16] [reserved_state_chunk] failed")
                    );
                }
            }
        });

        return new Tuple<>(savedClusterState, metadataVersion);
    }

    private void assertPipelinesNotSaved(CountDownLatch savedClusterState, AtomicLong metadataVersion) throws Exception {
        boolean awaitSuccessful = savedClusterState.await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        // This should succeed, nothing was reserved
        client().filterWithHeader(Map.of(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId.id()))
            .execute(PutPipelineTransportAction.TYPE, sampleRestRequest("my_ingest_pipeline_bad"))
            .get();
    }

    @AwaitsFix(bugUrl = "https://elasticco.atlassian.net/browse/ES-10518")
    @FixForMultiProject
    public void testErrorSaved() throws Exception {
        ensureGreen();
        var savedClusterState = setupClusterStateListenerForError(internalCluster().getMasterName());

        writeProjectJSONFiles(internalCluster().getMasterName(), projectErrorJSON);
        assertPipelinesNotSaved(savedClusterState.v1(), savedClusterState.v2());
    }

    private PutPipelineRequest sampleRestRequest(String id) throws Exception {
        var json = """
            {
               "description": "_description",
               "processors": [
                  {
                    "test" : {
                      "field": "_foo",
                      "value": "_bar"
                    }
                  }
               ]
            }""";

        try (
            var bis = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
            var parser = JSON.xContent().createParser(XContentParserConfiguration.EMPTY, bis);
            var builder = XContentFactory.contentBuilder(JSON)
        ) {
            builder.map(parser.map());
            return putJsonPipelineRequest(id, bytes(builder));
        }
    }

    private static class FakeProcessor implements Processor {
        private String type;
        private String tag;
        private String description;
        private Consumer<IngestDocument> executor;

        private FakeProcessor(String type, String tag, String description, Consumer<IngestDocument> executor) {
            this.type = type;
            this.tag = tag;
            this.description = description;
            this.executor = executor;
        }

        @Override
        public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
            executor.accept(ingestDocument);
            return ingestDocument;
        }

        @Override
        public String getType() {
            return type;
        }

        @Override
        public String getTag() {
            return tag;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }

    public static class CustomIngestTestPlugin extends IngestTestPlugin {
        @Override
        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            Map<String, Processor.Factory> processors = new HashMap<>();
            processors.put("test", (factories, tag, description, config) -> {
                String field = (String) config.remove("field");
                String value = (String) config.remove("value");
                return new FakeProcessor("test", tag, description, (ingestDocument) -> ingestDocument.setFieldValue(field, value));
            });

            return processors;
        }
    }
}
