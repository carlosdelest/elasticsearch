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
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.test.ESTestCase;

import java.util.regex.Pattern;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.sameInstance;

public class ServerlessBuildExtensionTests extends ESTestCase {

    @Override
    protected boolean enableWarningsCheck() {
        // This is a test involving HeaderWarning and Build statics, avoid loading them prematurely
        return false;
    }

    public void testHeaderWarningAgentMatchesServerlessBuildExtensionFormat() {
        final String formattedWarning = HeaderWarning.formatWarning("Blah blah blah");

        assertThat(formattedWarning, containsString("Elasticsearch-" + Build.current().hash()));
        assertTrue(
            "Warning on stateless should match version pattern provided by ServerlessBuildExtension (no semantic version)",
            HeaderWarning.warningHeaderPatternMatches(formattedWarning)
        );
    }

    public void testWarningValueFromHeaderWarningWorksWithServerlessBuildExtension() {
        final String s = randomAlphaOfLength(16);
        final String first = HeaderWarning.formatWarning(s);
        assertThat(HeaderWarning.extractWarningValueFromWarningHeader(first, false), equalTo(s));

        final String withPos = "[context][1:11] Blah blah blah";
        final String formatted = HeaderWarning.formatWarning(withPos);
        assertThat(HeaderWarning.extractWarningValueFromWarningHeader(formatted, true), equalTo("Blah blah blah"));

        final String withNegativePos = "[context][-1:-1] Blah blah blah";
        assertThat(
            HeaderWarning.extractWarningValueFromWarningHeader(HeaderWarning.formatWarning(withNegativePos), true),
            equalTo("Blah blah blah")
        );
    }

    public void testServerlessBuildExtensionUsedForBuild() {
        var hashOrUnknownPattern = Pattern.compile("(?:[a-f0-9]{7}(?:[a-f0-9]{33})?|unknown)");

        var serverlessBuild = new ServerlessBuildExtension().getCurrentBuild();
        var currentBuild = Build.current();
        assertThat(serverlessBuild, sameInstance(currentBuild));

        assertThat(serverlessBuild.version(), matchesPattern(hashOrUnknownPattern));
        assertThat(serverlessBuild.hash(), matchesPattern(hashOrUnknownPattern));
    }
}
