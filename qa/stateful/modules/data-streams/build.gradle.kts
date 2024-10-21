tasks {
    yamlRestTest {
        systemProperty("tests.rest.blacklist", listOf(
            "data_stream/10_basic/Get data stream and check DSL and ILM information",
            "data_stream/20_unsupported_apis/*",
            "data_stream/40_supported_apis/*"
        ).joinToString(","))
        systemProperty("yaml.rest.tests.set_num_nodes", "false")
    }
    javaRestTest {
        exclude("**/DataStreamDeleteLifecycleWithPermissionsRestIT.class")
        exclude("**/StandardVersusLogsIndexModeRandomDataChallengeRestIT.class")
        exclude("**/StandardVersusStandardReindexedIntoLogsDbChallengeRestIT.class")
        exclude("**/LogsDbVersusReindexedLogsDbChallengeRestIT.class")
        exclude("**/LogsDbVersusLogsDbReindexedIntoStandardModeChallengeRestIT.class")
        // Lossy params in source mapper are not allowed in Serverless but included in these tests
        exclude("**/LogsIndexModeCustomSettingsIT.class")
    }
}
