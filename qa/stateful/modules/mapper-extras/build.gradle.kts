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
    yamlRestTest {
        systemProperty("tests.rest.blacklist", listOf(
            // Disabled because tests configure number of replicas, which isn't supported in serverless
            // TODO: fix these test to not set number_of_replicas to zero.
            "rank_feature/10_*/*",
            "rank_feature/20_*/*",
            "rank_features/10_*/*",
            "scaled_float/10_*/*",
            "search-as-you-type/10_*/*",
            "search-as-you-type/20_*/*",
        ).joinToString(","))
    }
}
