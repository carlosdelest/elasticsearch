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

import org.elasticsearch.Build;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.internal.BuildExtension;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.security.CodeSource;
import java.util.Locale;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

public class ServerlessBuildExtension implements BuildExtension {
    private static final String FLAVOR = "serverless";

    private static final Build INSTANCE;
    static {
        final URL url = getCodeSourceLocation();
        final Tuple<String, String> hashAndDate = getHashAndDate(url);

        final String hash = hashAndDate.v1();
        final String date = hashAndDate.v2();

        var str = String.format(Locale.ROOT, "[serverless][%s][%s]", hash, date);

        INSTANCE = new Build(FLAVOR, Build.Type.DOCKER, hash, date, true, hash, hash, hash, str);
    }

    private static Tuple<String, String> getHashAndDate(URL codeSourceUrl) {
        if (codeSourceUrl != null && codeSourceUrl.getProtocol().equalsIgnoreCase("file")) {
            try (JarInputStream jar = new JarInputStream(FileSystemUtils.openFileURLStream(codeSourceUrl))) {
                Manifest manifest = jar.getManifest();
                // Manifest might be missing, or url does not point to a Jar file
                if (manifest != null) {
                    final var hash = manifest.getMainAttributes().getValue("Change");
                    final var date = manifest.getMainAttributes().getValue("Build-Date");
                    return Tuple.tuple(hash, date);
                }

            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return Tuple.tuple("unknown", "unknown");
    }

    private static URL getCodeSourceLocation() {
        final CodeSource codeSource = ServerlessBuildExtension.class.getProtectionDomain().getCodeSource();
        return codeSource == null ? null : codeSource.getLocation();
    }

    @Override
    public Build getCurrentBuild() {
        return INSTANCE;
    }
}
