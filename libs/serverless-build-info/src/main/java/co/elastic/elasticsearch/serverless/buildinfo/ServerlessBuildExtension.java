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

package co.elastic.elasticsearch.serverless.buildinfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Build;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.env.BuildVersion;
import org.elasticsearch.internal.BuildExtension;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.security.CodeSource;
import java.util.Locale;
import java.util.Map;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

public class ServerlessBuildExtension implements BuildExtension {
    private static final Logger logger = LogManager.getLogger(ServerlessBuildExtension.class);
    private static final String FLAVOR = "serverless";

    private static final Build INSTANCE;

    static {
        final Map<String, String> maniFestAttributes = resolveManifestAttributes(
            "Change",
            "Build-Date",
            "X-Compile-Elasticsearch-Snapshot"
        );
        final String hash = maniFestAttributes.getOrDefault("Change", "unknown");
        final String date = maniFestAttributes.getOrDefault("Build-Date", "unknown");
        final Boolean isSnapshot = "true".equals(maniFestAttributes.getOrDefault("X-Compile-Elasticsearch-Snapshot", "true"));
        var str = String.format(Locale.ROOT, "[serverless][%s][%s]", hash, date);
        INSTANCE = new Build(FLAVOR, Build.Type.DOCKER, hash, date, hash, null, isSnapshot, hash, hash, str);
    }

    /**
     * This build version implementation is intended to make version comparisons into no-ops.
     * In other words, this class models all serverless builds as equal. We do this because the
     * stateful implementation is intended to impose restrictions on users as they upgrade and
     * downgrade nodes. In serverless, however, Elastic is the sole operator, and we don't
     * need the same guardrails; other version classes such as {@link org.elasticsearch.TransportVersion}
     * and {@link org.elasticsearch.index.IndexVersion} handle the comparisons we need.
     */
    private static final BuildVersion SERVERLESS_BUILD_VERSION = new BuildVersion() {
        @Override
        public boolean onOrAfterMinimumCompatible() {
            return true;
        }

        @Override
        public boolean isFutureVersion() {
            return false;
        }

        @Override
        public int id() {
            return -1;
        }
    };

    private static Map<String, String> resolveManifestAttributes(String... keys) {
        Map<String, String> returnMap = Maps.newHashMapWithExpectedSize(keys.length);
        final URL url = getCodeSourceLocation();
        final String urlStr = url == null ? "" : url.toString();
        if (urlStr.startsWith("file:/")) {
            try (JarInputStream jar = new JarInputStream(FileSystemUtils.openFileURLStream(url))) {
                Manifest manifest = jar.getManifest();
                // Manifest might be missing, or url does not point to a Jar file
                if (manifest != null) {
                    for (String key : keys) {
                        String value = manifest.getMainAttributes().getValue(key);
                        if (value != null) {
                            returnMap.put(key, value);
                        }
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return returnMap;
    }

    private static URL getCodeSourceLocation() {
        final CodeSource codeSource = ServerlessBuildExtension.class.getProtectionDomain().getCodeSource();
        return codeSource == null ? null : codeSource.getLocation();
    }

    @Override
    public Build getCurrentBuild() {
        return INSTANCE;
    }

    @Override
    public boolean hasReleaseVersioning() {
        return false;
    }

    @Override
    public BuildVersion currentBuildVersion() {
        return SERVERLESS_BUILD_VERSION;
    }

    @Override
    public BuildVersion fromVersionId(int versionId) {
        // TODO: reenable this once we deal with PersistedCLusterState (ES-7343)
        // assert versionId == -1 : "do not create serverless build version with real version ID";
        if (versionId != -1) {
            logger.warn("Attempted to create a BuildVersion with stateful build id: [{}]", versionId);
        }
        return SERVERLESS_BUILD_VERSION;
    }
}
