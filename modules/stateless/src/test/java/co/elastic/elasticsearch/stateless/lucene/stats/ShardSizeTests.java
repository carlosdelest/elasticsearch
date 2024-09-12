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

package co.elastic.elasticsearch.stateless.lucene.stats;

import co.elastic.elasticsearch.stateless.api.ShardSizeStatsReader.ShardSize;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

public class ShardSizeTests extends AbstractWireSerializingTestCase<ShardSize> {

    @Override
    protected Writeable.Reader<ShardSize> instanceReader() {
        return ShardSize::from;
    }

    @Override
    protected ShardSize createTestInstance() {
        return randomShardSize();
    }

    @Override
    protected ShardSize mutateInstance(ShardSize instance) {
        return switch (randomInt(2)) {
            case 0 -> new ShardSize(
                randomValueOtherThan(instance.interactiveSizeInBytes(), ESTestCase::randomNonNegativeLong),
                instance.nonInteractiveSizeInBytes(),
                instance.primaryTerm(),
                instance.generation()
            );
            case 1 -> new ShardSize(
                instance.interactiveSizeInBytes(),
                randomValueOtherThan(instance.nonInteractiveSizeInBytes(), ESTestCase::randomNonNegativeLong),
                instance.primaryTerm(),
                instance.generation()
            );
            case 2 -> new ShardSize(
                instance.interactiveSizeInBytes(),
                instance.nonInteractiveSizeInBytes(),
                randomValueOtherThan(instance.primaryTerm(), ESTestCase::randomNonNegativeLong),
                randomValueOtherThan(instance.primaryTerm(), ESTestCase::randomNonNegativeLong)
            );
            default -> randomValueOtherThan(instance, ShardSizeTests::randomShardSize);
        };
    }

    public static ShardSize randomShardSize() {
        return new ShardSize(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
    }
}
