tasks {
    yamlRestTest {
        systemProperty("tests.rest.blacklist", listOf(
            "data_stream/10_basic/Get data stream and check DSL and ILM information",
            "data_stream/20_unsupported_apis/*",
            "data_stream/40_supported_apis/*",
            "data_stream/240_failure_store_info/Get failure store info from disabled failure store"
        ).joinToString(","))
        systemProperty("yaml.rest.tests.set_num_nodes", "false")
    }
    javaRestTest {
        exclude("**/DataStreamDeleteLifecycleWithPermissionsRestIT.class")

        filter {
            // Uses snapshot API not available as user API on serverless
            excludeTestsMatching("*.LogsDataStreamRestIT.testLogsDBSnapshotCreateRestoreMount")
        }
    }
}
