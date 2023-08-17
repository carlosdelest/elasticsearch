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

package co.elastic.elasticsearch.serverless.autoscaling;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.autoscaling.MlAutoscalingStats;

import java.io.IOException;

import static org.hamcrest.Matchers.matchesPattern;

public class MachineLearningTierMetricsTests extends AbstractWireSerializingTestCase<MachineLearningTierMetrics> {

    public static MachineLearningTierMetrics randomMachineLearningTierMetrics() {
        if (randomBoolean()) {
            return new MachineLearningTierMetrics(randomAutoscalingResources());
        }

        return new MachineLearningTierMetrics(
            randomAlphaOfLength(10),
            // TODO: ElasticsearchException does not implement equals/hashCode
            null
        );
    }

    public void testXContentWithException() throws IOException {
        MachineLearningTierMetrics mlTierMetrics = new MachineLearningTierMetrics(
            "timed out after",
            new ElasticsearchTimeoutException("timed out after [5s]")
        );

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            XContentBuilder content = mlTierMetrics.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            String contentAsString = Strings.toString(content);

            assertThat(contentAsString, matchesPattern(".*\"reason\"\\s*:\"timed out after\".*\\}.*"));
        }
    }

    @Override
    protected Writeable.Reader<MachineLearningTierMetrics> instanceReader() {
        return MachineLearningTierMetrics::new;
    }

    @Override
    protected MachineLearningTierMetrics createTestInstance() {
        return randomMachineLearningTierMetrics();
    }

    @Override
    protected MachineLearningTierMetrics mutateInstance(MachineLearningTierMetrics instance) throws IOException {
        return null;
    }

    private static MlAutoscalingStats randomAutoscalingResources() {
        return new MlAutoscalingStats(
            randomIntBetween(0, 100), // nodes
            randomNonNegativeLong(), // perNodeMemoryInBytes
            randomNonNegativeLong(), // modelMemoryInBytes
            randomIntBetween(0, 100), // processorsSum
            randomIntBetween(0, 100), // minNodes
            randomNonNegativeLong(), // extraSingleNodeModelMemoryInBytes
            randomIntBetween(0, 100), // extraSingleNodeProcessors
            randomNonNegativeLong(), // extraModelMemoryInBytes
            randomIntBetween(0, 100), // extraProcessors
            randomNonNegativeLong(), // removeNodeMemoryInBytes
            randomNonNegativeLong() // perNodeMemoryOverheadInBytes
        );
    }
}
