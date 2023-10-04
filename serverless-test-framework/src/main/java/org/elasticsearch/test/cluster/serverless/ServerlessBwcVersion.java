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

package org.elasticsearch.test.cluster.serverless;

import org.elasticsearch.test.cluster.util.Version;

/**
 * An extension of {@link Version} that reports a hard-coded "0.0.0" version used for resolution purposes but inherits normal behavior
 * for other methods like {@link #onOrAfter(String)} so that conditional logic that relies on knowing the stack version of the
 * distribution can still work as intended.
 */
public class ServerlessBwcVersion extends Version {
    private static final String SERVERLESS_BWC_STACK_VERSION_SYSPROP = "tests.serverless.bwc_stack_version";
    private static final Version INSTANCE;

    static {
        if (System.getProperty(SERVERLESS_BWC_STACK_VERSION_SYSPROP) != null) {
            Version stackVersion = Version.fromString(System.getProperty(SERVERLESS_BWC_STACK_VERSION_SYSPROP));
            INSTANCE = new ServerlessBwcVersion(
                stackVersion.getMajor(),
                stackVersion.getMinor(),
                stackVersion.getRevision(),
                stackVersion.getQualifier()
            );
        } else {
            INSTANCE = null;
        }
    }

    private ServerlessBwcVersion(int major, int minor, int revision, String qualifier) {
        super(major, minor, revision, qualifier);
    }

    public static Version instance() {
        if (INSTANCE == null) {
            throw new IllegalStateException(
                "Serverless BWC stack version undefined. Ensure you add usesBwcDistribution() to your build script."
            );
        }

        return INSTANCE;
    }

    @Override
    public String toString() {
        // Override version used for distribution resolution
        // See ServerlessDistributionDownloadPlugin
        return "0.0.0";
    }
}
