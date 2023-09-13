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

tasks {
    javaRestTest {
        // RemoteClusterAwareEqlRestTestCase uses its own system properties for configuring security
        systemProperty("tests.rest.cluster.remote.user", "stateful_rest_test_admin")
        systemProperty("tests.rest.cluster.remote.password", "x-pack-test-password")

        // EQL Stats endpoint is disabled in serverless
        exclude("**/EqlStatsIT.class")

        // Assertions are failing due to discrepencies in exceptions when security is enabled
        exclude("**/EqlRestValidationIT.class")
    }
}
