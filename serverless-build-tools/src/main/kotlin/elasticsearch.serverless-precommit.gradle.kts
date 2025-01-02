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

import org.elasticsearch.gradle.internal.conventions.precommit.LicenseHeadersTask
import org.elasticsearch.gradle.internal.precommit.CheckstylePrecommitPlugin
import org.elasticsearch.gradle.internal.precommit.CopyCheckStyleConfTask
import org.elasticsearch.gradle.internal.precommit.LoggerUsagePrecommitPlugin

/*
 * Overrides to "precommit" task behavior specific for private code development
 */
allprojects {
    tasks.withType<LicenseHeadersTask>().configureEach {
        additionalLicense("CONFI", "Confidential", "ELASTICSEARCH CONFIDENTIAL")
        approvedLicenses = listOf("Confidential")
    }

    plugins.withType<CheckstylePrecommitPlugin> {
        // Configure additional Checkstyle suppressions
        var rootDirectory = rootDir
        var buildDirectory = layout.buildDirectory.get().asFile

        tasks.withType<CopyCheckStyleConfTask>().configureEach {
            doLast {
                getFs().copy {
                    from("${rootDirectory}/serverless-build-tools/src/main/resources/additional_checkstyle_suppressions.xml")
                    into("${buildDirectory}/checkstyle")
                }
            }
        }
    }

    plugins.withType<LoggerUsagePrecommitPlugin> {
        // Fix up the logger usage check dependency
        dependencies.add("loggerUsagePlugin", "org.elasticsearch.test:logger-usage")
    }

    tasks.withType<Javadoc>().configureEach {
        enabled = false
    }
}
