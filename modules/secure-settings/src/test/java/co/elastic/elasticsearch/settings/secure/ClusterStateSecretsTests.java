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
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.LocallyMountedSecrets;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.AbstractNamedWriteableTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class ClusterStateSecretsTests extends AbstractNamedWriteableTestCase<ClusterStateSecrets> {

    private static final String MOUNTED_SETTINGS = """
        {
            "metadata": {
                "version": "1",
                "compatibility": "8.7.0"
            },
            "secrets": {
                "foo": "bar",
                "goo": "baz"
            }
        }
        """;

    private LocallyMountedSecrets locallyMountedSecrets;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        Environment environment = newEnvironment();
        writeTestFile(environment.configDir().resolve("secrets").resolve("secrets.json"), MOUNTED_SETTINGS);
        locallyMountedSecrets = new LocallyMountedSecrets(environment);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        locallyMountedSecrets.close();
    }

    public void testGetSettings() throws Exception {
        ClusterStateSecrets clusterStateSecrets = new ClusterStateSecrets(1, locallyMountedSecrets);

        assertThat(clusterStateSecrets.getSettings().getSettingNames(), containsInAnyOrder("foo", "goo"));
        assertThat(clusterStateSecrets.getSettings().getString("foo").toString(), equalTo("bar"));
        assertThat(clusterStateSecrets.getSettings().getString("goo").toString(), equalTo("baz"));

        locallyMountedSecrets.close();

        assertThat(clusterStateSecrets.getSettings().getSettingNames(), containsInAnyOrder("foo", "goo"));
        assertThat(clusterStateSecrets.getSettings().getString("foo").toString(), equalTo("bar"));
        assertThat(clusterStateSecrets.getSettings().getString("goo").toString(), equalTo("baz"));
    }

    public void testFileSettings() throws Exception {
        ClusterStateSecrets clusterStateSecrets = new ClusterStateSecrets(1, locallyMountedSecrets);

        assertThat(clusterStateSecrets.getSettings().getFile("foo").readAllBytes(), equalTo("bar".getBytes(StandardCharsets.UTF_8)));
    }

    public void testSerialize() throws Exception {
        ClusterStateSecrets clusterStateSecrets = new ClusterStateSecrets(1, locallyMountedSecrets);

        final BytesStreamOutput out = new BytesStreamOutput();
        clusterStateSecrets.writeTo(out);
        final ClusterStateSecrets fromStream = new ClusterStateSecrets(out.bytes().streamInput());

        assertThat(fromStream.getVersion(), equalTo(clusterStateSecrets.getVersion()));

        assertThat(fromStream.getSettings().getSettingNames(), hasSize(2));
        assertThat(fromStream.getSettings().getSettingNames(), containsInAnyOrder("foo", "goo"));

        assertEquals(clusterStateSecrets.getSettings().getString("foo"), fromStream.getSettings().getString("foo"));
        assertTrue(fromStream.getSettings().isLoaded());
    }

    public void testToXContentChunked() {
        ClusterStateSecrets clusterStateSecrets = new ClusterStateSecrets(1, locallyMountedSecrets);

        // we never serialize anything to x-content
        assertFalse(clusterStateSecrets.toXContentChunked(EMPTY_PARAMS).hasNext());
    }

    public void testToString() {
        ClusterStateSecrets clusterStateSecrets = new ClusterStateSecrets(1, locallyMountedSecrets);

        assertThat(clusterStateSecrets.toString(), equalTo("ClusterStateSecrets{[all secret]}"));
    }

    public void testClose() throws Exception {
        ClusterStateSecrets clusterStateSecrets = new ClusterStateSecrets(1, locallyMountedSecrets);

        SecureSettings settings = clusterStateSecrets.getSettings();
        assertThat(settings.getSettingNames(), containsInAnyOrder("foo", "goo"));
        assertThat(settings.getString("foo").toString(), equalTo("bar"));
        assertThat(settings.getString("goo").toString(), equalTo("baz"));

        settings.close();

        // we can close the copy
        assertThat(settings.getSettingNames(), containsInAnyOrder("foo", "goo"));
        assertThat(settings.getString("foo"), nullValue());
        assertThat(settings.getString("goo"), nullValue());

        // fetching again returns a fresh object
        SecureSettings settings2 = clusterStateSecrets.getSettings();
        assertThat(settings2.getSettingNames(), containsInAnyOrder("foo", "goo"));
        assertThat(settings2.getString("foo").toString(), equalTo("bar"));
        assertThat(settings2.getString("goo").toString(), equalTo("baz"));
    }

    @Override
    protected ClusterStateSecrets createTestInstance() {
        return new ClusterStateSecrets(
            randomLong(),
            new ClusterStateSecrets.Secrets(
                randomMap(
                    0,
                    3,
                    () -> Tuple.tuple(
                        randomAlphaOfLength(10),
                        new ClusterStateSecrets.Entry(
                            randomAlphaOfLength(15).getBytes(Charset.defaultCharset()),
                            randomByteArrayOfLength(8)
                        )
                    )
                )
            )
        );
    }

    @Override
    protected ClusterStateSecrets mutateInstance(ClusterStateSecrets instance) throws IOException {
        return new ClusterStateSecrets(instance.getVersion() + 1L, instance.getSettings());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(new NamedWriteableRegistry.Entry(ClusterStateSecrets.class, ClusterStateSecrets.TYPE, ClusterStateSecrets::new))
        );
    }

    @Override
    protected Class<ClusterStateSecrets> categoryClass() {
        return ClusterStateSecrets.class;
    }

    private void writeTestFile(Path path, String contents) throws IOException {
        Path tempFilePath = createTempFile();

        Files.writeString(tempFilePath, contents);
        Files.createDirectories(path.getParent());
        Files.move(tempFilePath, path, StandardCopyOption.ATOMIC_MOVE);
    }
}
