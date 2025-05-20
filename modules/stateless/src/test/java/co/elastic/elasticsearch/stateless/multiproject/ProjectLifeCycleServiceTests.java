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

package co.elastic.elasticsearch.stateless.multiproject;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.utils.TransferableCloseables;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.hamcrest.Matchers;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService.BUCKET_SETTING;
import static org.elasticsearch.env.Environment.PATH_REPO_SETTING;

public class ProjectLifeCycleServiceTests extends ESTestCase {

    public void testProjectLease() throws Exception {
        try (var closeable = new TransferableCloseables()) {
            var pathHome = LuceneTestCase.createTempDir().toAbsolutePath();
            var repoPath = LuceneTestCase.createTempDir();
            var nodeSettings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), pathHome)
                .put(PATH_REPO_SETTING.getKey(), repoPath)
                .put(BUCKET_SETTING.getKey(), repoPath)
                .build();
            var clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            var threadPool = new TestThreadPool("test", Stateless.statelessExecutorBuilders(Settings.EMPTY, true));
            closeable.add(() -> TestThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));
            var clusterService = closeable.add(ClusterServiceUtils.createClusterService(threadPool));
            var client = new NoOpNodeClient(threadPool);
            var environment = newEnvironment(nodeSettings);
            var xContentRegistry = xContentRegistry();
            var repoService = new RepositoriesService(
                nodeSettings,
                clusterService,
                Map.of(
                    FsRepository.TYPE,
                    metadata -> new FsRepository(
                        metadata,
                        environment,
                        xContentRegistry,
                        clusterService,
                        BigArrays.NON_RECYCLING_INSTANCE,
                        new RecoverySettings(nodeSettings, clusterSettings)
                    ) {
                        @Override
                        protected BlobStore createBlobStore() throws Exception {
                            final String location = REPOSITORIES_LOCATION_SETTING.get(getMetadata().settings());
                            final Path locationFile = environment.resolveRepoDir(location);
                            return new FsBlobStore(bufferSize, locationFile, isReadOnly());
                        }
                    }
                ),
                Map.of(),
                threadPool,
                client,
                List.of()
            );
            var objectStoreService = new ObjectStoreService(
                nodeSettings,
                repoService,
                threadPool,
                clusterService,
                TestProjectResolvers.DEFAULT_PROJECT_ONLY
            );
            closeable.add(objectStoreService);
            objectStoreService.start();

            var cluster1Service = new ProjectLifeCycleService(objectStoreService, threadPool);
            var cluster1Uuid = UUIDs.randomBase64UUID();
            cluster1Service.setClusterUuid(cluster1Uuid);
            var cluster2Service = new ProjectLifeCycleService(objectStoreService, threadPool);
            var cluster2Uuid = UUIDs.randomBase64UUID();
            cluster2Service.setClusterUuid(cluster2Uuid);
            var projectId = randomUniqueProjectId();

            // Releasing an unassigned project's lease fails
            var listener = new SubscribableListener<Boolean>();
            if (randomBoolean()) {
                cluster1Service.releaseProjectLease(projectId, listener);
            } else {
                cluster2Service.releaseProjectLease(projectId, listener);
            }
            assertFalse(safeAwait(listener));
            // one cluster can take the lease
            listener = new SubscribableListener<>();
            cluster1Service.acquireProjectLease(projectId, listener);
            assertTrue(safeAwait(listener));
            // re-acquire has no effect
            listener = new SubscribableListener<>();
            cluster1Service.acquireProjectLease(projectId, listener);
            assertTrue(safeAwait(listener));
            // another cluster cannot acquire the lease
            listener = new SubscribableListener<>();
            cluster2Service.acquireProjectLease(projectId, listener);
            assertFalse(safeAwait(listener));
            // another cluster cannot release the lease
            listener = new SubscribableListener<>();
            cluster2Service.releaseProjectLease(projectId, listener);
            assertFalse(safeAwait(listener));
            // owner can release
            listener = new SubscribableListener<>();
            cluster1Service.releaseProjectLease(projectId, listener);
            assertTrue(safeAwait(listener));
            // Releasing an unassigned project's lease fails
            listener = new SubscribableListener<>();
            if (randomBoolean()) {
                cluster1Service.releaseProjectLease(projectId, listener);
            } else {
                cluster2Service.releaseProjectLease(projectId, listener);
            }
            assertFalse(safeAwait(listener));
            // Both try to acquire
            var listener1 = new SubscribableListener<Boolean>();
            var listener2 = new SubscribableListener<Boolean>();
            cluster1Service.acquireProjectLease(projectId, listener1);
            cluster2Service.acquireProjectLease(projectId, listener2);
            var successfulCount = new AtomicInteger();
            var unsuccessfulCount = new AtomicInteger();
            var failureCount = new AtomicInteger();
            listener1.addListener(ActionListener.wrap(success -> {
                if (success) successfulCount.incrementAndGet();
                else unsuccessfulCount.incrementAndGet();
            }, e -> failureCount.incrementAndGet()));
            listener2.addListener(ActionListener.wrap(success -> {
                if (success) successfulCount.incrementAndGet();
                else unsuccessfulCount.incrementAndGet();
            }, e -> failureCount.incrementAndGet()));
            assertBusy(() -> assertThat(2, Matchers.equalTo(successfulCount.get() + unsuccessfulCount.get() + failureCount.get())));
            assertThat(successfulCount.get(), Matchers.equalTo(1));
            assertThat(unsuccessfulCount.get(), Matchers.lessThanOrEqualTo(1));
            if (unsuccessfulCount.get() == 0) {
                assertThat(failureCount.get(), Matchers.equalTo(1));
            } else {
                assertThat(failureCount.get(), Matchers.equalTo(0));
            }
        }
    }
}
