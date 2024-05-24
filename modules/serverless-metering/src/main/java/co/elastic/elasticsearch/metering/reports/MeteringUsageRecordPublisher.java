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

package co.elastic.elasticsearch.metering.reports;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface MeteringUsageRecordPublisher extends Closeable {
    MeteringUsageRecordPublisher NOOP_REPORTER = new MeteringUsageRecordPublisher() {
        @Override
        public void sendRecords(List<UsageRecord> records) {}

        @Override
        public void close() {}
    };

    void sendRecords(List<UsageRecord> records) throws IOException, InterruptedException;
}
