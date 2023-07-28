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
    private static final String VERSION = "serverless";

    private static final Build INSTANCE;
    static {
        final String hash;
        final String date;

        final URL url = getCodeSourceLocation();
        final String urlStr = url == null ? "" : url.toString();
        if (urlStr.startsWith("file:/")) {
            try (JarInputStream jar = new JarInputStream(FileSystemUtils.openFileURLStream(url))) {
                Manifest manifest = jar.getManifest();
                hash = manifest.getMainAttributes().getValue("Change");
                date = manifest.getMainAttributes().getValue("Build-Date");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            hash = "unknown";
            date = "unknown";
        }
        var str = String.format(Locale.ROOT, "[serverless][%s][%s]", hash, date);

        INSTANCE = new Build(FLAVOR, Build.Type.DOCKER, hash, date, true, VERSION, VERSION, VERSION, str);
    }

    static URL getCodeSourceLocation() {
        final CodeSource codeSource = ServerlessBuildExtension.class.getProtectionDomain().getCodeSource();
        return codeSource == null ? null : codeSource.getLocation();
    }

    @Override
    public Build getCurrentBuild() {
        return INSTANCE;
    }
}
