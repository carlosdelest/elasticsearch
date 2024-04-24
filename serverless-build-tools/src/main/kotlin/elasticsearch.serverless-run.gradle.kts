import org.elasticsearch.gradle.testclusters.ElasticsearchCluster
import org.elasticsearch.gradle.testclusters.RunTask
import org.elasticsearch.gradle.testclusters.TestDistribution

val testClusters = project.extensions.getByName("testClusters") as NamedDomainObjectContainer<ElasticsearchCluster>
val repoDir = file("${buildDir}/runTask/repo")

val runCluster by testClusters.registering {
    setTestDistribution(TestDistribution.DEFAULT)
    numberOfNodes = 3
    setting("path.repo", repoDir.absolutePath)
    setting("stateless.enabled", "true")
    setting("stateless.object_store.type", "fs")
    setting("stateless.object_store.bucket", "stateless")
    setting("stateless.object_store.base_path", "base_path")
    setting("ingest.geoip.downloader.enabled", "false")
    setting("xpack.ml.enabled", "true")
    setting("xpack.profiling.enabled", "false")
    setting("xpack.security.enabled", "true")
    setting("xpack.watcher.enabled", "false")
    setting("xpack.security.operator_privileges.enabled", "true")
    setting("serverless.sigterm.timeout", "1s")
    setting("serverless.sigterm.poll_interval", "1s")
    keystore("bootstrap.password", "password")
    extraConfigFile("operator_users.yml", file("${rootDir}/serverless-build-tools/src/main/resources/operator_users.yml"));
    extraConfigFile("service_tokens", file("${rootDir}/serverless-build-tools/src/main/resources/service_tokens"));
    user(mapOf("username" to "elastic-admin", "password" to "elastic-password"))
    user(mapOf("username" to "elastic-user", "password" to "elastic-password", "role" to "superuser"))
    nodes["runCluster-0"].setting("node.roles", "[master,remote_cluster_client,ingest,index]")
    nodes["runCluster-0"].setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
    nodes["runCluster-0"].setting("xpack.searchable.snapshot.shared_cache.region_size", "256K")
    nodes["runCluster-1"].setting("node.roles", "[master,remote_cluster_client,search]")
    nodes["runCluster-1"].setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
    nodes["runCluster-1"].setting("xpack.searchable.snapshot.shared_cache.region_size", "256K")
    nodes["runCluster-2"].setting("node.roles", "[master,remote_cluster_client,ml,transform]")
}

tasks {
    register("run", RunTask::class) {
        useCluster(runCluster)
        description = "Runs serverless elasticsearch in the foreground"
        group = "Verification"

        impliesSubProjects = true
    }
}
