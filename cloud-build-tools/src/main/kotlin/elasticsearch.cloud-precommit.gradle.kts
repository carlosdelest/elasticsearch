import org.elasticsearch.gradle.internal.conventions.precommit.LicenseHeadersTask
import org.elasticsearch.gradle.internal.precommit.CheckstylePrecommitPlugin
import org.elasticsearch.gradle.internal.precommit.LoggerUsagePrecommitPlugin

/*
 * Overrides to "precommit" task behavior specific for private code development
 */
allprojects {
    tasks.withType<LicenseHeadersTask>().configureEach {
        // We don't need to enforce license headers on private code
        onlyIf { false }
    }

    plugins.withType<CheckstylePrecommitPlugin> {
        // Configure additional Checkstyle suppressions
        tasks.named("copyCheckstyleConf") {
            doLast {
                copy {
                    from("${rootDir}/cloud-build-tools/src/main/resources/additional_checkstyle_suppressions.xml")
                    into("${buildDir}/checkstyle")
                }
            }
        }
    }

    plugins.withType<LoggerUsagePrecommitPlugin> {
        // Fix up the logger usage check dependency
        dependencies.add("loggerUsagePlugin", "org.elasticsearch.test:logger-usage")
    }
}
