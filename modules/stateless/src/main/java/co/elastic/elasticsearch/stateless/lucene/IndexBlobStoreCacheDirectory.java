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

package co.elastic.elasticsearch.stateless.lucene;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;
import co.elastic.elasticsearch.stateless.cache.reader.CacheBlobReader;
import co.elastic.elasticsearch.stateless.cache.reader.MeteringCacheBlobReader;
import co.elastic.elasticsearch.stateless.cache.reader.ObjectStoreCacheBlobReader;
import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;

public class IndexBlobStoreCacheDirectory extends BlobStoreCacheDirectory {

    public IndexBlobStoreCacheDirectory(StatelessSharedBlobCacheService cacheService, ShardId shardId) {
        super(cacheService, shardId);
    }

    void updateMetadata(Map<String, BlobLocation> blobLocations, long dataSetSizeInBytes) {
        assert assertCompareAndSetUpdatingCommitThread(null, Thread.currentThread());
        try {
            currentMetadata = blobLocations;
            currentDataSetSizeInBytes = dataSetSizeInBytes;
        } finally {
            assert assertCompareAndSetUpdatingCommitThread(Thread.currentThread(), null);
        }
    }

    @Override
    protected CacheBlobReader getCacheBlobReader(BlobLocation location) {
        return doGetCacheBlobReader(getBlobContainer(location.primaryTerm()), location.blobName());
    }

    private MeteringCacheBlobReader doGetCacheBlobReader(BlobContainer blobContainer, String blobName) {
        return new MeteringCacheBlobReader(
            new ObjectStoreCacheBlobReader(
                blobContainer,
                blobName,
                getCacheService().getRangeSize(),
                getCacheService().getShardReadThreadPoolExecutor()
            ),
            totalBytesReadFromObjectStore::add
        );
    }

    @Override
    public CacheBlobReader getCacheBlobReaderForWarming(BlobLocation location) {
        return doGetCacheBlobReaderForWarming(getBlobContainer(location.primaryTerm()), location.blobName());
    }

    private MeteringCacheBlobReader doGetCacheBlobReaderForWarming(BlobContainer blobContainer, String blobName) {
        assert ThreadPool.assertCurrentThreadPool(Stateless.PREWARM_THREAD_POOL, ThreadPool.Names.GENERIC);
        return new MeteringCacheBlobReader(
            new ObjectStoreCacheBlobReader(blobContainer, blobName, getCacheService().getRangeSize(), EsExecutors.DIRECT_EXECUTOR_SERVICE),
            totalBytesWarmedFromObjectStore::add
        );
    }

    public BlobStoreCacheDirectory createBlobStoreCacheDirectoryForWarming(StatelessCompoundCommit preWarmCommit) {
        var preWarmDirectory = new BlobStoreCacheDirectory(cacheService, shardId) {
            @Override
            protected CacheBlobReader getCacheBlobReader(BlobLocation blobLocation) {
                return doGetCacheBlobReader(getBlobContainer(blobLocation.primaryTerm()), blobLocation.blobName());
            }

            @Override
            public CacheBlobReader getCacheBlobReaderForWarming(BlobLocation location) {
                return doGetCacheBlobReaderForWarming(getBlobContainer(location.primaryTerm()), location.blobName());
            }
        };
        preWarmDirectory.currentMetadata = preWarmCommit.commitFiles();
        preWarmDirectory.currentDataSetSizeInBytes = preWarmCommit.getAllFilesSizeInBytes();
        return preWarmDirectory;
    }

    public static IndexBlobStoreCacheDirectory unwrapDirectory(final Directory directory) {
        Directory dir = directory;
        while (dir != null) {
            if (dir instanceof IndexBlobStoreCacheDirectory blobStoreCacheDirectory) {
                return blobStoreCacheDirectory;
            } else if (dir instanceof IndexDirectory indexDirectory) {
                return indexDirectory.getBlobStoreCacheDirectory();
            } else if (dir instanceof FilterDirectory) {
                dir = ((FilterDirectory) dir).getDelegate();
            } else {
                dir = null;
            }
        }
        var e = new IllegalStateException(directory.getClass() + " cannot be unwrapped as " + IndexBlobStoreCacheDirectory.class);
        assert false : e;
        throw e;
    }
}
