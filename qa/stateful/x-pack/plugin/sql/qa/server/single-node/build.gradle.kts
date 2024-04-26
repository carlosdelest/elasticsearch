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
        // RemoteClusterAwareSqlRestTestCase uses its own system properties for configuring security
        systemProperty("tests.rest.cluster.multi.user", "stateful_rest_test_admin")
        systemProperty("tests.rest.cluster.multi.password", "x-pack-test-password")

        // Stats endpoint is disabled in Serverless
        exclude("**/RestSqlUsageIT.class")
        // Frozen indices not supported in Serverless
        exclude("**/JdbcDocFrozenCsvSpecIT.class")
        exclude("**/JdbcFrozenCsvSpecIT.class")
        // AwaitsFix: https://github.com/elastic/elasticsearch-serverless/issues/1501
        exclude("**/RestSqlIT.class")
        // AwaitsFix: https://github.com/elastic/elasticsearch-serverless/issues/1611
        exclude("**/JdbcShardFailureIT.class")
    }
}
