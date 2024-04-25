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

package co.elastic.elasticsearch.api.validation;

import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;

import java.util.List;

import static co.elastic.elasticsearch.api.filtering.CommonTestPublicSettings.THREAD_CONTEXT;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamLifecycleValidatorTests extends ESTestCase {

    public void testDisablingLifecycleAllowedForOperator() {
        try (ThreadContext.StoredContext ignored = THREAD_CONTEXT.stashContext()) {
            // set operator privileges
            THREAD_CONTEXT.putHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY, AuthenticationField.PRIVILEGE_CATEGORY_VALUE_OPERATOR);

            DataStreamLifecycleValidator validator = new DataStreamLifecycleValidator(THREAD_CONTEXT);
            validator.validateLifecycle(null);
            validator.validateLifecycle(randomLifecycle(true));
            validator.validateLifecycle(randomLifecycle(false));
        }
    }

    public void testLifecycleValidation() {
        try (ThreadContext.StoredContext ignored = THREAD_CONTEXT.stashContext()) {
            DataStreamLifecycleValidator validator = new DataStreamLifecycleValidator(THREAD_CONTEXT);
            validator.validateLifecycle(null);
            validator.validateLifecycle(randomLifecycle(true));
            var e = expectThrows(IllegalArgumentException.class, () -> validator.validateLifecycle(randomLifecycle(false)));
            assertThat(e.getMessage(), equalTo("Data stream lifecycle cannot be disabled in serverless, please remove 'enabled=false'"));
        }
    }

    private DataStreamLifecycle randomLifecycle(boolean enabled) {
        DataStreamLifecycle.Builder builder = DataStreamLifecycle.newBuilder().enabled(enabled);
        if (randomBoolean()) {
            builder.dataRetention(TimeValue.timeValueDays(randomIntBetween(10, 100)));
        }
        if (randomBoolean()) {
            builder.downsampling(
                new DataStreamLifecycle.Downsampling(
                    List.of(
                        new DataStreamLifecycle.Downsampling.Round(
                            TimeValue.timeValueDays(randomIntBetween(1, 5)),
                            new DownsampleConfig(new DateHistogramInterval("10m"))
                        )
                    )
                )
            );
        }
        return builder.build();
    }
}
