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

package co.elastic.elasticsearch.metering.sampling.action;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class CollectClusterSamplesActionTests extends ESTestCase {
    private static final long ONE_GB = 1024L * 1024L * 1024L;

    public void testExtrapolatedMemoryOnPartialSuccess() {
        var searchMemory = randomIntBetween(1, 100) * ONE_GB;
        var indexMemory = randomIntBetween(1, 100) * ONE_GB;

        var searchNodes = randomIntBetween(2, 100);
        var searchNodeSuccess = randomIntBetween(1, searchNodes - 1);

        var indexNodes = randomIntBetween(2, 100);
        var indexNodeSuccess = randomIntBetween(1, indexNodes - 1);

        var resp = new CollectClusterSamplesAction.Response(
            searchMemory,
            indexMemory,
            null,
            null,
            null,
            searchNodes,
            searchNodes - searchNodeSuccess,
            indexNodes,
            indexNodes - indexNodeSuccess
        );

        assertThat(resp.isPartialSuccess(), is(true));
        assertThat(resp.getExtrapolatedSearchTierMemorySize(), is(searchMemory / searchNodeSuccess * searchNodes));
        assertThat(resp.getExtrapolatedIndexTierMemorySize(), is(indexMemory / indexNodeSuccess * indexNodes));
    }

    public void testExtrapolatedMemoryOnSuccess() {
        var searchMemory = randomIntBetween(1, 100) * ONE_GB;
        var indexMemory = randomIntBetween(1, 100) * ONE_GB;

        var searchNodes = randomIntBetween(2, 100);
        var indexNodes = randomIntBetween(2, 100);

        var resp = new CollectClusterSamplesAction.Response(searchMemory, indexMemory, null, null, null, searchNodes, 0, indexNodes, 0);

        assertThat(resp.isPartialSuccess(), is(false));
        assertThat(resp.getExtrapolatedSearchTierMemorySize(), is(searchMemory));
        assertThat(resp.getExtrapolatedIndexTierMemorySize(), is(indexMemory));
    }
}
