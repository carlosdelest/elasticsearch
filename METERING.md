# Running telemetry locally Elasticsearch Serverless

please read the main documentation on apm metering in  [APM Metering](/elasticsearch/modules/apm/METERING.md).

in order to run serverless node with apm telemetry you need edit your apm.gradle file (see tha main documentation first)

edit your `~/.gradle/init.d/apm.gradle`
```groovy
rootProject {
    if (project.name == 'elasticsearch-serverless' && Boolean.getBoolean('metrics.enabled')) {
        afterEvaluate {
            testClusters.matching { it.name == "runCluster" }.configureEach {
                setting 'telemetry.metrics.enabled', 'true'
                setting 'telemetry.tracing.enabled', 'true'
                extraConfigFile 'secrets/secrets.json', file("PATH_YOUR_CONFIGS/secrets.json")
                setting 'telemetry.agent.server_url', 'https://TODO-REPLACE-URL.apm.eastus2.staging.azure.foundit.no:443'
            }
        }
    }
}
```

`secrets.json`
```json
{
  "metadata": {
    "version": "1",
    "compatibility": "8.4.0"
  },
  "string_secrets": {
    "telemetry.secret_token": "yoursecret"
  }
}
```

The example use:
```
./gradlew :run -Dmetrics.enabled=true
```

