dependencyResolutionManagement {
    versionCatalogs {
        create("buildLibs") {
            from(files("../elasticsearch/gradle/build.versions.toml"))
        }
    }
}
