# Stateful Elasticsearch test suites

This project houses the execution of test suites for modules that
exist in the upstream stateful Elasticsearch project. This allows
for running existing tests against a serverless deployment with
some caveats and exceptions (see below).

## Enabling stateful test suite in serverless

1. Project must be using the new test-clusters framework using
   JUnit rules, not the Gradle plugin. That is, it must not be
   using any "legacy" Gradle plugins.
2. Ensure that the `elasticsearch.internal-test-artifact`, plugin
   is applied to the project build script.
3. Add the project to [`settings.gradle.kts`](/settings.gradle.kts):
   ```kotlin
   includeStatefulTests(":path:to:project", includeYamlTests = true, includeJavaTests = true)
   ```
   You must explicitly state whether YAML and/or Java test suites
   will be included.

You can now run the tests as you would have in the stateful project
by prefixing the project path with `:qa:stateful` (ex: if the
project path was `:modules:foo` you will the Java REST tests via
`./gradlew :qa:stateful:modules:foo:javaRestTest`).

## Customizing or disabling tests

The above will execute all tests in the included test project using
a serverless deployment. There may be some tests that are not
applicable to serverless and should be excluded, or other required
build setup that we don't inherit. If you need to customized the
project in any way you can add a `build.gradle.kts` file in the
project directory under `qa/stateful` with any required overrides.
For example, if you need to exclude some YAML test cases:

**qa/stateful/modules/foo/build.gradle.kts**

```kotlin
tasks {
    yamlRestTest {
        systemProperty("tests.rest.blacklist", listOf(
            "foo/bar/*",
            "foo/20_baz/*"
        ).joinToString(","))
    }
}
```

## Caveats and test compatibility

For the most part, any customer configuration applied to the local
test cluster in the stateful test suite is also applied to the
serverless cluster, with some exceptions:

1. Security is always enabled and cannot be disabled. Unless
   overridden, the test REST client will be configured to use a
   default superuser with operator privileges.
2. The number of nodes in the cluster cannot be altered, nor can
   new nodes be added. Serverless clusters have a predictable
   topology (search/index data tiers, etc).
3. Settings cannot be applied to individual nodes.
4. Non-string keystore entries are unsupported.

### Ignoring missing settings in serverless

Some settings are simply not available in serverless. Entire modules
like ILM, Watcher, etc are completely missing in serverless. As a result,
any attempt to set settings registered by those modules will cause
the test cluster to fail to start. Rather than require upstream tests
to account for this, or conditionally apply these settings, we simply
ignore them when creating our serverless cluster. The list of these
disallowed settings [exists here](/serverless-test-framework/src/main/java/org/elasticsearch/test/cluster/serverless/local/core/CoreServerlessLocalClusterSpecBuilder.java#L50).
