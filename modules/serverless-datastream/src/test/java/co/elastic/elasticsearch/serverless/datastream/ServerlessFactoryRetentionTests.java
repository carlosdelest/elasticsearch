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

package co.elastic.elasticsearch.serverless.datastream;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ServerlessFactoryRetentionTests extends ESTestCase {

    public void testDefaultSettings() {
        ServerlessFactoryRetention serverlessFactoryRetention = new ServerlessFactoryRetention();
        assertThat(serverlessFactoryRetention.getDefaultRetention(), nullValue());
        assertThat(serverlessFactoryRetention.getMaxRetention(), nullValue());
    }

    public void testValidCombination() {
        var defaultValue = randomBoolean() ? TimeValue.MINUS_ONE : randomTimeValue(7, 30, TimeUnit.DAYS);
        var maxValue = randomBoolean() ? TimeValue.MINUS_ONE : randomTimeValue(30, 90, TimeUnit.DAYS);
        Settings s = Settings.builder()
            .put(ServerlessFactoryRetention.DATA_STREAMS_DEFAULT_RETENTION_SETTING.getKey(), defaultValue.getStringRep())
            .put(ServerlessFactoryRetention.DATA_STREAMS_MAX_RETENTION_SETTING.getKey(), maxValue.getStringRep())
            .build();
        assertThat(ServerlessFactoryRetention.DATA_STREAMS_DEFAULT_RETENTION_SETTING.get(s), equalTo(defaultValue));
        assertThat(ServerlessFactoryRetention.DATA_STREAMS_MAX_RETENTION_SETTING.get(s), equalTo(maxValue));
    }

    public void testInvalidSettingsTooSmall() {
        Settings s = Settings.builder()
            .put(ServerlessFactoryRetention.DATA_STREAMS_DEFAULT_RETENTION_SETTING.getKey(), "5s")
            .put(ServerlessFactoryRetention.DATA_STREAMS_MAX_RETENTION_SETTING.getKey(), "5s")
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ServerlessFactoryRetention.DATA_STREAMS_DEFAULT_RETENTION_SETTING.get(s)
        );
        assertThat(e.getMessage(), containsString("Global retention values should be greater than "));
        e = expectThrows(IllegalArgumentException.class, () -> ServerlessFactoryRetention.DATA_STREAMS_MAX_RETENTION_SETTING.get(s));
        assertThat(e.getMessage(), containsString("Global retention values should be greater than "));
    }

    public void testInvalidCombination() {
        Settings s = Settings.builder()
            .put(ServerlessFactoryRetention.DATA_STREAMS_DEFAULT_RETENTION_SETTING.getKey(), "5d")
            .put(ServerlessFactoryRetention.DATA_STREAMS_MAX_RETENTION_SETTING.getKey(), "1d")
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ServerlessFactoryRetention.DATA_STREAMS_DEFAULT_RETENTION_SETTING.get(s)
        );
        assertThat(e.getMessage(), containsString("Default global retention [5d] cannot be greater than the max global retention [1d]."));
        e = expectThrows(IllegalArgumentException.class, () -> ServerlessFactoryRetention.DATA_STREAMS_MAX_RETENTION_SETTING.get(s));
        assertThat(e.getMessage(), containsString("Default global retention [5d] cannot be greater than the max global retention [1d]."));
    }
}
