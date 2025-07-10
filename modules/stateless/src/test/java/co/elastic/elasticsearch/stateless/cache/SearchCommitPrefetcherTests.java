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

package co.elastic.elasticsearch.stateless.cache;

import co.elastic.elasticsearch.stateless.commits.BatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.BlobFile;
import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Map;

import static co.elastic.elasticsearch.stateless.cache.SearchCommitPrefetcher.getPendingRangesToPrefetch;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class SearchCommitPrefetcherTests extends ESTestCase {
    public void testInitialGetPendingRangesToPrefetch() {
        {
            var rangesToPrefetch = computeRangesToPrefetch(
                SearchCommitPrefetcher.BCCPreFetchedOffset.ZERO,
                1,
                luceneFile(1, 10, 10),
                luceneFile(1, 20, 20)
            );

            assertThat(
                rangesToPrefetch,
                // Only expected to fetch up to the latest file
                equalTo(Map.of(blobFile(1), ByteRange.of(0, 40)))
            );
        }

        {
            var rangesToPrefetch = computeRangesToPrefetch(
                new SearchCommitPrefetcher.BCCPreFetchedOffset(termAndGen(1, 1), 30),
                1,
                luceneFile(1, 0, 10),
                luceneFile(1, 10, 20),
                luceneFile(1, 40, 20)
            );

            assertThat(
                rangesToPrefetch,
                // Expect to prefetch the contiguous range between the latest prefetched offset and the new file (even if there's a hole)
                equalTo(Map.of(blobFile(1), ByteRange.of(30, 60)))
            );
        }

        {
            var rangesToPrefetch = computeRangesToPrefetch(
                SearchCommitPrefetcher.BCCPreFetchedOffset.ZERO,
                2,
                luceneFile(1, 0, 10),
                luceneFile(1, 10, 20),
                luceneFile(2, 10, 20)
            );
            assertThat(
                rangesToPrefetch,
                // Since this is the first commit that we prefetch, and it uses files stored in two BCCs
                // we have to fetch ranges from both regions
                equalTo(Map.of(blobFile(1), ByteRange.of(0, 30), blobFile(2), ByteRange.of(0, 30)))
            );
        }
    }

    public void testGetPendingRangesWithMaxBCCToPrefetch() {
        {
            var rangesToPrefetch = computeRangesToPrefetch(
                SearchCommitPrefetcher.BCCPreFetchedOffset.ZERO,
                1,
                luceneFile(1, 0, 10),
                luceneFile(1, 10, 20),
                luceneFile(2, 0, 20)
            );
            assertThat(rangesToPrefetch, equalTo(Map.of(blobFile(1), ByteRange.of(0, 30))));
        }

        {
            var rangesToPrefetch = computeRangesToPrefetch(
                SearchCommitPrefetcher.BCCPreFetchedOffset.ZERO,
                2,
                luceneFile(1, 0, 10),
                luceneFile(1, 10, 20),
                luceneFile(2, 0, 20)
            );
            assertThat(rangesToPrefetch, equalTo(Map.of(blobFile(1), ByteRange.of(0, 30), blobFile(2), ByteRange.of(0, 20))));
        }
    }

    public void testGetPendingRangesToPrefetchOnSubsequentCommits() {
        var bccPreFetchedOffset = SearchCommitPrefetcher.BCCPreFetchedOffset.ZERO;

        assertThat(
            computeRangesToPrefetch(bccPreFetchedOffset, 1, luceneFile(1, 0, 10), luceneFile(1, 10, 20)),
            // Only expected to fetch up to the latest file
            equalTo(Map.of(blobFile(1), ByteRange.of(0, 30)))
        );

        bccPreFetchedOffset = new SearchCommitPrefetcher.BCCPreFetchedOffset(termAndGen(1, 1), 30);

        assertThat(
            computeRangesToPrefetch(bccPreFetchedOffset, 1, luceneFile(1, 0, 10), luceneFile(1, 10, 20), luceneFile(1, 30, 20)),
            // Previously we fetched up to offset 30, now we only need to fetch the latest file
            equalTo(Map.of(blobFile(1), ByteRange.of(30, 50)))
        );

        bccPreFetchedOffset = new SearchCommitPrefetcher.BCCPreFetchedOffset(termAndGen(1, 1), 50);

        assertThat(
            computeRangesToPrefetch(
                bccPreFetchedOffset,
                2,
                luceneFile(1, 0, 10),
                luceneFile(1, 10, 20),
                luceneFile(1, 30, 20),
                luceneFile(1, 50, 100),
                luceneFile(2, 0, 150)
            ),
            // Somehow we missed fetching one file from the BCC with generation 1 and now there's a new generation 2,
            // so we have to prefetch the missing range from 1 and the new file
            equalTo(Map.of(blobFile(1), ByteRange.of(50, 150), blobFile(2), ByteRange.of(0, 150)))
        );

        bccPreFetchedOffset = new SearchCommitPrefetcher.BCCPreFetchedOffset(termAndGen(1, 2), 150);
        assertThat(
            computeRangesToPrefetch(
                bccPreFetchedOffset,
                2,
                luceneFile(1, 0, 10),
                luceneFile(1, 10, 20),
                luceneFile(1, 30, 20),
                luceneFile(1, 50, 100),
                luceneFile(2, 0, 150)
            ),
            is(equalTo(Map.of()))
        );
    }

    private Map<BlobFile, ByteRange> computeRangesToPrefetch(
        SearchCommitPrefetcher.BCCPreFetchedOffset bccPreFetchedOffset,
        long maxBCCGenerationToPrefetch,
        BlobLocation... blobLocations
    ) {
        assertThat(blobLocations.length, greaterThan(0));
        return getPendingRangesToPrefetch(bccPreFetchedOffset, maxBCCGenerationToPrefetch, Arrays.asList(blobLocations));
    }

    private BlobLocation luceneFile(long generation, long offset, long length) {
        return new BlobLocation(blobFile(generation), offset, length);
    }

    private BlobFile blobFile(long generation) {
        return new BlobFile(BatchedCompoundCommit.blobNameFromGeneration(generation), termAndGen(1, generation));
    }

    private static PrimaryTermAndGeneration termAndGen(long term, long generation) {
        return new PrimaryTermAndGeneration(term, generation);
    }
}
