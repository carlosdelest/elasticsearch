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

package co.elastic.elasticsearch.metering.codec;

import co.elastic.elasticsearch.metering.reporter.RawStorageAccumulator;
import co.elastic.elasticsearch.stateless.api.DocValuesFormatFactory;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;

public class RawStorageDocValuesFormatFactory implements DocValuesFormatFactory {

    @Override
    public DocValuesFormat createDocValueFormat(DocValuesFormat parentCodecDocValuesFormat) {
        return new RawStorageDocValuesFormat(parentCodecDocValuesFormat);
    }

    private static class RawStorageDocValuesFormat extends DocValuesFormat {
        private final DocValuesFormat innerDocValuesFormat;

        RawStorageDocValuesFormat(DocValuesFormat innerDocValuesFormat) {
            super(innerDocValuesFormat.getName());
            this.innerDocValuesFormat = innerDocValuesFormat;
        }

        private static class RawStorageDocValuesConsumer extends DocValuesConsumer {
            private static final Logger logger = LogManager.getLogger(RawStorageDocValuesConsumer.class);
            private final DocValuesConsumer delegate;
            private final SegmentWriteState segmentWriteState;

            private RawStorageDocValuesConsumer(DocValuesConsumer delegate, SegmentWriteState segmentWriteState) {
                this.delegate = delegate;
                this.segmentWriteState = segmentWriteState;
            }

            @Override
            public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
                if (RawStorageAccumulator.RA_STORAGE_KEY.equals(field.name)) {
                    var values = valuesProducer.getNumeric(field);
                    long rawSize = 0;
                    long docCount = 0;
                    while (values.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        long rawDocValue = values.longValue();
                        // Currently we do not meter RA-S when replaying from translog (ES-8577).
                        // However, due to a bug we recorded the default raw size (-1 meaning not metered) in this case.
                        if (rawDocValue >= 0) {
                            rawSize += rawDocValue;
                            docCount = docCount + 1;
                        }
                    }
                    long avgRawSizePerDoc = docCount == 0 ? 0 : rawSize / docCount;
                    logger.trace("addNumericField: [{}] is [{}] (size: [{}], docs: [{}])", field.name, avgRawSizePerDoc, rawSize, docCount);
                    segmentWriteState.segmentInfo.putAttribute(RawStorageAccumulator.RA_STORAGE_AVG_KEY, Long.toString(avgRawSizePerDoc));
                }
                delegate.addNumericField(field, valuesProducer);
            }

            @Override
            public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
                delegate.addBinaryField(field, valuesProducer);
            }

            @Override
            public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
                delegate.addSortedField(field, valuesProducer);
            }

            @Override
            public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
                delegate.addSortedNumericField(field, valuesProducer);
            }

            @Override
            public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
                delegate.addSortedSetField(field, valuesProducer);
            }

            @Override
            public void close() throws IOException {
                delegate.close();
            }
        }

        @Override
        public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
            return new RawStorageDocValuesConsumer(innerDocValuesFormat.fieldsConsumer(state), state);
        }

        @Override
        public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
            return innerDocValuesFormat.fieldsProducer(state);
        }
    }
}
