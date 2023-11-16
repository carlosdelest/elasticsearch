tasks {
    yamlRestTest {
        systemProperty("tests.rest.blacklist", listOf(
            "downsample/60_settings/Downsample datastream with tier preference"
        ).joinToString(","))
    }
}
