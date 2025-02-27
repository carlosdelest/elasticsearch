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

package co.elastic.elasticsearch.settings.secure;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class SecureClusterStateSettingsTests extends ESTestCase {
    MockSecureSettings mockSecureSettings = new MockSecureSettings();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        // SecureSettings in cluster state are handled as file settings (get the byte array) both can be fetched as
        // string or file
        mockSecureSettings.setFile("foo", "bar".getBytes(StandardCharsets.UTF_8));
        mockSecureSettings.setFile("goo", "baz".getBytes(StandardCharsets.UTF_8));
    }

    public void testGetSettings() throws Exception {
        SecureClusterStateSettings secureClusterStateSettings = new SecureClusterStateSettings(mockSecureSettings);
        assertThat(secureClusterStateSettings.getSettingNames(), containsInAnyOrder("foo", "goo"));
        assertThat(secureClusterStateSettings.getString("foo").toString(), equalTo("bar"));
        assertThat(new String(secureClusterStateSettings.getFile("goo").readAllBytes(), StandardCharsets.UTF_8), equalTo("baz"));
    }

    public void testSerialize() throws Exception {
        SecureClusterStateSettings secureClusterStateSettings = new SecureClusterStateSettings(mockSecureSettings);

        final BytesStreamOutput out = new BytesStreamOutput();
        secureClusterStateSettings.writeTo(out);
        final SecureClusterStateSettings fromStream = new SecureClusterStateSettings(out.bytes().streamInput());

        assertThat(fromStream.getSettingNames(), hasSize(2));
        assertThat(fromStream.getSettingNames(), containsInAnyOrder("foo", "goo"));

        assertEquals(secureClusterStateSettings.getString("foo"), fromStream.getString("foo"));
        assertThat(new String(fromStream.getFile("goo").readAllBytes(), StandardCharsets.UTF_8), equalTo("baz"));
        assertTrue(fromStream.isLoaded());
    }
}
