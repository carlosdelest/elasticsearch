tasks {
    yamlRestTest {
        systemProperty("tests.rest.blacklist", listOf(
            "data_stream/10_basic/Get data stream and check DSL and ILM information",
            "data_stream/20_unsupported_apis/*",
            "data_stream/40_supported_apis/*",
            "data_stream/240_data_stream_settings/*" // the only allowed data stream settings are blocked on serverless
        ).joinToString(","))
        systemProperty("yaml.rest.tests.set_num_nodes", "false")
        // The ILM plugin is not included in serverless, so disabling the ILM history store through a setting would cause an error.
        systemProperty("yaml.rest.tests.disable_ilm_history", "false")
    }
    javaRestTest {
        exclude("**/DataStreamDeleteLifecycleWithPermissionsRestIT.class")

        filter {
            // Uses snapshot API not available as user API on serverless
            excludeTestsMatching("*.LogsDataStreamRestIT.testLogsDBSnapshotCreateRestoreMount")
        }
    }
}
