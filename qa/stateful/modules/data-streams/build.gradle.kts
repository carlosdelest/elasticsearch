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
        exclude("**/DataStreamLifecyclePermissionsRestIT.class")
    }
}
