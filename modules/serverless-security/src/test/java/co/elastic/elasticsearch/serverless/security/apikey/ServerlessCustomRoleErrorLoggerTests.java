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

package co.elastic.elasticsearch.serverless.security.apikey;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.audit.logfile.CapturingLogger;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class ServerlessCustomRoleErrorLoggerTests extends ESTestCase {

    public void testNoLogWhenNoRoleDescriptors() throws IllegalAccessException {
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO, null);
        ServerlessCustomRoleErrorLogger.logCustomRoleErrors(
            "potato",
            "payload",
            p -> fail("I should not get called"),
            randomBoolean() ? null : List.of(),
            logger
        );
        final List<String> logEvents = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(logEvents, is(empty()));
        logEvents.clear();
    }

    public void testNoLogWhenInvalidRoleDescriptors() throws IllegalAccessException {
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO, null);
        ServerlessCustomRoleErrorLogger.logCustomRoleErrors(
            "potato",
            "payload",
            p -> fail("I should not get called"),
            List.of(new RoleDescriptor("name", new String[] { "invalid_cluster_privilege" }, null, null)),
            logger
        );
        final List<String> logEvents = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(logEvents, is(empty()));
        logEvents.clear();
    }

    public void testNoLogWhenValid() throws IllegalAccessException {
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO, null);
        ServerlessCustomRoleErrorLogger.logCustomRoleErrors(
            "potato",
            "payload",
            p -> {},
            List.of(new RoleDescriptor("name", new String[] { "all" }, null, null)),
            logger
        );
        final List<String> logEvents = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(logEvents, is(empty()));
        logEvents.clear();
    }

    public void testLogWhenInvalid() throws IllegalAccessException {
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO, null);
        RuntimeException expectedException = new RuntimeException("tomato");
        ServerlessCustomRoleErrorLogger.logCustomRoleErrors(
            "potato",
            "payload",
            p -> { throw expectedException; },
            List.of(new RoleDescriptor("name", new String[] { "all" }, null, null)),
            logger
        );
        final List<String> logEvents = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(logEvents, contains(containsString("Invalid custom role descriptors in [potato].")));
        logEvents.clear();
    }
}
