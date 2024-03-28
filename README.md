# Elasticsearch Serverless

This repository is home for code specific to the "serverless" distribution of Elasticsearch.

## Getting started

### Cloning the repository

Serverless Elasticsearch is currently built with "core" [Elasticsearch](https://github.com/elastic/elasticsearch) as a
baseline, with additional modules and changes as necessary. Elasticsearch is brought in as a source build dependency via
a [Git submodule](https://www.git-scm.com/book/en/v2/Git-Tools-Submodules). As such, interacting with the code in this
repository is a bit different, depending on whether you are modifying serverless code, or core Elasticsearch code. To
start, when doing a clone, you'll need to instruct Git to also clone any submodules, like so:

```shell
git clone --recurse-submodules git@github.com:elastic/elasticsearch-serverless.git
```

Should you forget to do this, you'll observe that the `elasticsearch` subdirectory will be empty and the build will
fail. You can always initialize the submodule after the fact by running:

```shell
git submodule update --init
```

### Updating submodules

As with cloning, just doing a `git pull` will not automatically update submodules unless the `--recurse-submodules`
flag is passed. For convenience, it's recommended to set the `submodule.recurse` option so that this flag is implicitly
passed for all supported git operations.

```shell
git config submodule.recurse true
```

If you want to pull in new _upstream_ changes from Elasticsearch you will need to explicitly do so:

```shell
git submodule update --remote
```

The above command will fetch from the upstream remote and update the local submodule commit reference. This only updates
your local repository, so you'll also need to commit and push this change. After updating the submodule commit, running
`git status` should show something like this:

```shell
On branch main
Your branch is up to date with 'origin/main'.

Changes not staged for commit:
  (use "git add <file>..." to update what will be committed)
  (use "git restore <file>..." to discard changes in working directory)
	modified:   elasticsearch (new commits)
```

At which point the typical `git add`, `git commit` and `git push` workflow would be used to push these changes to a
remote branch. Similarly, you can revert uncommitted changes by simply doing `git checkout elasticsearch`.

### Working on submodule code

Working on code in the submodule is for most intents and purposes no different than working directly in the
Elasticsearch repository. All git commands inside the `elasticsearch` directory work just as if you were working in a
discrete clone of that repository. You can create and change branches, push upstream, and manage remotes, just as you
would normally. The primary difference is that when pull the root repository, the `elasticsearch` directory will be
placed in a **detached HEAD** state. Before committing any changes to the submodule repository you will first need to switch
to a branch. In general, you'll typically create a new branch, commit your change, then push that branch to your
personal fork from which you will create a pull request. Essentially, no different than how we work on core
Elasticsearch today.

### Importing into IntelliJ

The serverless project shares all the same IDE integration as core Elasticsearch so in terms of tooling and IntelliJ
integration, please refer to the [Elasticsearch contributor guide](/elasticsearch/CONTRIBUTING.md).

### Running and debugging locally

You can build a local platform-specific serverless distribution as you would with Elasticsearch via:

```shell
./gradlew localDistro
```

Serverless Elasticsearch isn't designed to be typically run in a single-node configuration. Generally, search,
ingest and ML nodes are separated. For convenience, you can run a local 3 node cluster by just using the `run`
task.

```shell
./gradlew :run
```

The cluster will have security enabled. There are 2 users, `elastic-admin` and `elastic-user`, both with the password `elastic-password`.

If you want to debug your running cluster you can add the `--debug-jvm` flag. Before doing so ensure you start
three debug run configurations in IntelliJ. These are created for you on project import. You'll need to start
"Debug Elasticsearch" as well as the matching "node 2" and "node 3" run configurations to start debuggers for
each running node.

#### Customizing the cluster

If need be, you can pass additional settings to the test cluster via system properties with the `tests.es`
prefix. For example:

```shell
./gradlew :run -Dtests.es.xpack.security.enabled=false
```

If you need to make further customizations, the cluster definition for this task [lives here](serverless-build-tools/src/main/kotlin/elasticsearch.serverless-run.gradle.kts).

### Deploy snapshot into in QA environment

To deploy a branch snapshot into QA

1. Trigger a new build from this pipeline https://buildkite.com/elastic/elasticsearch-serverless-deploy-qa
   This deploys a snapshot from the selected branch into our QA environment (see https://docs.elastic.dev/serverless/qa)
   by publishing a docker snapshot into our internal docker registry and then using the serverless project api to deploy that snapshot to our serverless platform QA environment.

   By default a project of type `elasticsearch` is deployed. If you want to deploy a different project type (`observability` or `security`) you can pass the project type as environment variable when triggering the pipeline above via:

   ```
   PROJECT_TYPE=observability
   ```

   The url of the deployed ess instance is shown in an info box top of the build. e.g. https://buildkite.com/elastic/elasticsearch-serverless-deploy-qa/builds/3#annotation-ess-public-url
   The encrypted user password is also shown in an info box top of the build. e.g. https://buildkite.com/elastic/elasticsearch-serverless-deploy-qa/builds/4#annotation-ess-password-encrypted

   Store both values in a env variable:
   ```
   export ESS_PUBLIC_URL=<URL_FROM_BUILD_INFO_BOX>
   export ESS_ROOT_USERNAME=testing-internal
   export ESS_ROOT_PASSWORD_ENCRYPTED="<ENCRYPTED_PASSWORD_FROM_BUILD_INFO_BOX>"
   ```

2. to decrypt the elastic userpassword you need to resolve the encryption key from vault and then decrypt the password
   ```
   > vault read -field private-key secret/elasticsearch-team/delivery-encryption > key.pem;

   export ESS_ROOT_PASSWORD=$(echo "$ESS_ROOT_PASSWORD_ENCRYPTED" | openssl base64 -d | openssl pkeyutl -decrypt -inkey key.pem)
   ```
4. Now you should be able to access the ess instance via curl
   ```

   curl -k -u $ESS_ROOT_USERNAME:$ESS_ROOT_PASSWORD $ESS_PUBLIC_URL
    {
    "name" : "serverless",
    "cluster_name" : "b510f56edb87490aa00597c01e5f5a6a",
    "cluster_uuid" : "2M0GMx2iS66pXc_fboiJfA",
    "version" : {
    "number" : "8.11.0",
    "build_flavor" : "serverless",
    "build_type" : "docker",
    "build_hash" : "00000000",
    "build_date" : "2023-10-31",
    "build_snapshot" : false,
    "lucene_version" : "9.7.0",
    "minimum_wire_compatibility_version" : "8.11.0",
    "minimum_index_compatibility_version" : "8.11.0"
    },
    "tagline" : "You Know, for Search"
    }
    ```

After its usage cleanup the ess deployment by triggering the undeploy-qa pipeline at https://buildkite.com/elastic/elasticsearch-serverless-undeploy-qa. Choose the same branch used for deployment earlier. By
default the last deployed project from that branch. To undeploy a specific project, the project id must be passed via environment variables to the pipeline as PROJECT_ID.

Alternatively you can use the buildkite commandline interface to undeploy your project by running

```
# for deleting the latest deployment from this branch
> bk build create --pipeline elastic/elasticsearch-serverless-undeploy-qa --branch my-branch
```

### Update an elasticsearch serverless platform QA environment deployment

To update an existing elasticsearch serverless QA deployment the update-dev buildkite pipeline at https://buildkite.com/elastic/elasticsearch-serverless-update-qa can be used.

The pipeline must be triggered from the same branch as the deploy qa was executed. The last deployed project by https://buildkite.com/elastic/elasticsearch-serverless-deploy-qa will be updated.

### Running end to end tests against a kubernetes based serverless platform QA environment

We have a set of end to end tests that run against a kubernetes based serverless platform dev environment.
Those tests live in the `:qa:e2e-test` subproject and are run against a QA deployment in this
pipeline: https://buildkite.com/elastic/elasticsearch-serverless-e2e-tests-qa

The end to end tests can be run locally against a kubernetes based serverless platform QA deployment by

1. Deploying a branch snapshot into QA as described above
2. Resolve deployment url as decribed above and export it as `ESS_PUBLIC_URL` env environment
3. Resolve the elasticsearch encrypted api key from the buildkite UI and export to `ESS_API_KEY_ENCRYPTED`
4. Resolve the ci encryption key from elastic vault (https://secrets.elastic.co:8200) and store it in a file
5. Export the encoded api key in an env variable.
6. Run `./gradlew :qa:e2e-test:javaRestTest` to invoke the end to end tests.

```
> export ESS_PUBLIC_URL=<your ess deployment url>
> export ESS_API_KEY_ENCRYPTED=<the encrypted ess api key of the deployment>
> vault read -field private-key secret/elasticsearch-team/delivery-encryption > key.pem
> export ESS_API_KEY_ENCODED=$(echo $ESS_API_KEY_ENCRYPTED | openssl base64 -d | openssl pkeyutl -decrypt -inkey key.pem)`
> ./gradlew :qa:e2e-test:javaRestTest
```

### Building and running locally with docker

Using the `run` Gradle task is the most convenient way to locally run serverless Elasticsearch. If you want
you can also run via Docker if need be.

The Serverless Elasticsearch x86 image can be built using the command:

```shell
./gradlew buildDockerImage
```

If you need the ARM image (e.g., for Apple M1 processor), it can be built instead with:

```shell
./gradlew buildAarch64DockerImage
```

Run the following command once, to create the docker network that will be used by the docker instances:

```shell
docker network create elastic
```

#### Running a cluster with multiple instances

If you would like to use a file system object store (see below if you would like to run with S3 instead), first create an empty permissive tmpfs directory `/tmp/objectstore` to use as a file system object store:

```shell
rm -rf /tmp/objectstore ; mkdir /tmp/objectstore ; chmod a+rw -R /tmp/objectstore
```

And then if you would like to run a cluster with 3 instances, with a separate Index and Search instance, you can run:

```shell
docker run --rm -d --name es01 --net elastic -p 9200:9200 -p 9300:9300 -e ES_JAVA_OPTS="-Xms1g -Xmx1g" -e node.name=es01 -e cluster.initial_master_nodes=es01,es02,es03 -e discovery.seed_hosts=es02,es03 -e node.roles='["master","index"]' -e xpack.security.enabled=false -e cluster.name=stateless -e stateless.enabled=true -e xpack.searchable.snapshot.shared_cache.size=1gb -e stateless.object_store.type=fs -e stateless.object_store.bucket=stateless -e path.repo=/objectstore -v /tmp/objectstore:/objectstore:z elasticsearch-serverless
docker run --rm -d --name es02 --net elastic -p 9202:9202 -p 9302:9302 -e ES_JAVA_OPTS="-Xms1g -Xmx1g" -e node.name=es02 -e cluster.initial_master_nodes=es01,es02,es03 -e discovery.seed_hosts=es01,es03 -e node.roles='["master","search"]' -e xpack.security.enabled=false -e cluster.name=stateless -e stateless.enabled=true -e xpack.searchable.snapshot.shared_cache.size=1gb -e stateless.object_store.type=fs -e stateless.object_store.bucket=stateless -e path.repo=/objectstore -v /tmp/objectstore:/objectstore:z elasticsearch-serverless
docker run --rm -d --name es03 --net elastic -p 9203:9203 -p 9303:9303 -e ES_JAVA_OPTS="-Xms1g -Xmx1g" -e node.name=es03 -e cluster.initial_master_nodes=es01,es02,es03 -e discovery.seed_hosts=es01,es02 -e node.roles='["master"]' -e xpack.security.enabled=false -e cluster.name=stateless -e stateless.enabled=true -e stateless.object_store.type=fs -e stateless.object_store.bucket=stateless -e path.repo=/objectstore -v /tmp/objectstore:/objectstore:z elasticsearch-serverless
```

**WARNING**: Do not attempt to change the mounted host directory to an important directory on your system unless you understand the implications of the `-v` and `:z` docker configuration above.

You can access the logs of an instance using:

```shell
docker logs -f es01
```

You can stop the instances using:

```shell
docker container stop es01 es02 es03
```

If you would like to use a S3 bucket instead of a file system object store, you will first need to create a `secrets/secrets.json`
file for your S3 credentials. It should look like this:

```json
{
    "metadata": {
        "version": "1",
        "compatibility": "8.6.0"
    },
    "string_secrets": {
        "s3.client.test.access_key": "...",
        "s3.client.test.secret_key": "...",
        "s3.client.test.session_token": "..."
    }
}
```

GCP clients take credentials in the form of a file. Since a file may have an encoding other than UTF-8,
such secrets must be base64-encoded and put into a different section of the secrets file:

```json
{
  "metadata": {
    "version": "1",
    "compatibility": "8.6.0"
  },
  "file_secrets": {
    "gcs.client.test.credentials_file": "eyJtZXNzYWdlIjoibm90IHRoZSByZWFsIGdjcyBmb3JtYXQifQo="
  }
}
```

Assuming that the `./secrets` directory is in your working directory, you can run Elasticsearch with:

```shell
docker run --rm -d --name es01 --net elastic -p 9200:9200 -p 9300:9300 -e ES_JAVA_OPTS="-Xms1g -Xmx1g" -e node.name=es01 -e cluster.initial_master_nodes=es01,es02,es03 -e discovery.seed_hosts=es02,es03 -e node.roles='["master","index"]' -e xpack.security.enabled=false -e cluster.name=stateless -e stateless.enabled=true -e stateless.object_store.type=s3 -e stateless.object_store.client=test -e stateless.object_store.bucket=... -v $(realpath ./secrets) elasticsearch-serverless
docker run --rm -d --name es02 --net elastic -p 9202:9202 -p 9302:9302 -e ES_JAVA_OPTS="-Xms1g -Xmx1g" -e node.name=es02 -e cluster.initial_master_nodes=es01,es02,es03 -e discovery.seed_hosts=es01,es03 -e node.roles='["master","search"]' -e xpack.security.enabled=false -e cluster.name=stateless -e stateless.enabled=true -e stateless.object_store.type=s3 -e stateless.object_store.client=test -e stateless.object_store.bucket=... -v $(realpath ./secrets) elasticsearch-serverless
docker run --rm -d --name es03 --net elastic -p 9203:9203 -p 9303:9303 -e ES_JAVA_OPTS="-Xms1g -Xmx1g" -e node.name=es03 -e cluster.initial_master_nodes=es01,es02,es03 -e discovery.seed_hosts=es01,es02 -e node.roles='["master"]' -e xpack.security.enabled=false -e cluster.name=stateless -e stateless.enabled=true -e stateless.object_store.type=s3 -e stateless.object_store.client=test -e stateless.object_store.bucket=... -v $(realpath ./secrets) elasticsearch-serverless
```

#### Running autoscaling E2E tests

* Create your own cloud dev enviroment based on the [k8s-gitops-control](https://github.com/elastic/k8s-gitops-control-plane#local-development-environment) instructions.

There are a lot of prerequisites that need to be installed on your local machine in order to create an environment with just a `make dev-deploy` command. Reach out to the `platform-engineering-productivity` team if you have any issues.

We test by default on AWS, see [Setting up AWS development environment](#setting-up-aws-development-environment) for the tips how to make sure you use the correct AWS configuration for creating own EKS cluster.

* Create a dev Python environment for running [es-benchmarks](https://github.com/elastic/elasticsearch-benchmarks).

Reach out to the `es-perf` team if you have issues installing `pyenv` and building `esbench`.
Tip: don't forget to activate the environment with `source .venv/bin/activate`.

* Use [serverless-on-k8s](https://github.com/elastic/elasticsearch-benchmarks/tree/master/tools/serverless-on-k8s) tool for deploying `elasticsearch-serverless` Docker images
for performance testing.

Switch `AWS_PROFILE` to `ecdev` by running `export AWS_PROFILE=ecdev` before using `serverless-on-k8s` in order to communicate with your EKS cluster.

* Configure `kubectl` to use the `esbench` configuration `./serverless-on-k8s.py configure-kubectl --k8s-cluster=<your_k8s_cluster_name>`

`your_k8s_cluster_name` is the cluster name you set in the `infra.env` file in the `k8s-gitops-control-plane` repo

* Create a load balancer `./serverless-on-k8s.py create-external-loadbalancer`

* Create a new ES namespace `./serverless-on-k8s.py create`

The script will ask you whether you want to edit the deployment descriptors for ES services. Do edit them, because the defaults are too high for your development clusters.
Set the disk limits to `100Gb`, memory limit to `8Gb` and the CPU limit to `4`.

When the namespace is created, the script will print out an environment id and an URL that you will need to use for further interactions with the cluster.

* Start `esbench`.

Switch `AWS_PROFILE` to `elastic-dev` by running `export AWS_PROFILE=elastic-dev` before using `esbench`, so it can use the `elastic-dev` account. Don't forget update credentials with `okta-awscli -o default --profile=elastic-dev`.

The `create` command prints out instructions how start `esbench`. It should like something like

```
Invoke esbench with: esbench start --use-case=external --env-id=09ea5254-457a-45bb-b15f-eb2fc59d2abe --params='{"elasticsearch.url": "https://35.195.111.0", "elasticsearch.username": "esbench", "elasticsearch.password": "super-secret-password", "cloud.vendor": "gcp", "cloud.region": "europe-west1", "client.options": {"default": {"timeout": 60, "headers": {"X-Found-Cluster": "09ea5254-457a-45bb-b15f-eb2fc59d2abe.es"}}}, "track.name": "http_logs", "track.params": {}, "track.challenge": "append-no-conflicts-index-only"}' --user-tags="project:serverless"
```

It's a good idea to modify the track name and its parameters based on the type workload you're resting. For example, you can do something like `"track.name": "nyc_taxis", "track.params"`. For example,  "track.params": {"number_of_shards": 5, "bulk_indexing_clients": 16, "number_of_replicas": 1, "ingest_percentage": 10}`

* Make sure the dry-run mode for the autoscaler is disabled

Run `kubectl get eas -n <esbench_env_id>` and check that the autoscaler is actually active. If it's not, disable the dry-run mode with `kubectl annotate eas es common.k8s.elastic.co/dry-run- -n <esbench_env_id>`. You can also run `kubectl --namespace=<esbench_env_id> get pods` and see whether the autoscaler started scaling pods to the initial state. If pods are being scaled, wait until all the pods are active and the cluster is in a stable state.

* Execute the benchmark with `esbench execute --env-id=<esbench_env_id>`

* Start monitoring the autoscaler and k8s pods to see the cluster scaling up and down


Check es-ingest and es-search pods and their status to see whether the cluster is being scaled

```
kubectl --namespace=<esbench_env_id> get pods
```

Check the status of the es-ingest and es-search pods from the point of view of the autoscaler

```
kubectl --namespace=<esbench_env_id> get eas/es -o=jsonpath='{.status}'  | jq
```

Check the autoscaler logs to see that the autoscaler is checking the metrics and trying to autoscale the cluster

```
kubectl --namespace=<esbench_env_id> logs -l app.kubernetes.io/name=elasticsearch-autoscaler -n elastic-system -f
```

Check exposed Autoscaling metrics
```
curl -k -u esbench:super-secret-password -H 'X-Found-Cluster: <esbench_env_id>.es' https://<esbench_es_url>/_internal/serverless/autoscaling | jq .
```

* Track the progress of the benchmark

Login to the machine running the benchmark `esbench ssh --env-id=<esbench_env_id>` and look at the rally logs `~/.rally/logs/rally.log`

#### Setting up AWS development environment

* Set `CSP_TYPE` environment variable `export CSP_TYPE=aws`.
* Set `AWS_DEFAULT_REGION` environment variable `export AWS_DEFAULT_REGION=eu-west-1` # Set to the region that is closer to you.
* Install and configure `okta-awscli` tool according to the [docs](https://github.com/elastic/cloud/blob/master/wiki/AWS.md).
* Latest `okta-awscli` requires Python 3.10. Make sure to update it, e.g. Ubuntu uses Python 3.8 by default.
* Remove `factor: FIDO` from the `.okta-aws` config you had it set previously.
* `esbench` and `gitops-control-plane` use different AWS profiles and accounts. You have to set up *both*. So, your `.okta-aws` should look like this

```
[default]
base-url = elastic.okta.com
app-link = https://elastic.okta.com/home/amazon_aws/0oabplp058WZVHLWM1t7/272
username = firstname.lastname@elastic.co
duration = 14400
role = arn:aws:iam::946960629917:role/ElasticDeveloper

[ecdev]
base-url = elastic.okta.com
app-link = https://elastic.okta.com/home/amazon_aws/0oaaibfi8ztk6zoVR1t7/272
username = firstname.lastname@elastic.co
duration = 43200
role = arn:aws:iam::284141849446:role/saml/saml_cloud_developers
```

* Double check the `app-link` in your `ecdev` config in `.okta-aws`. It should be set to `https://elastic.okta.com/home/amazon_aws/0oaaibfi8ztk6zoVR1t7/272`. If you set the `app-link` correctly, `okta-awscli` will assign `arn:aws:iam::284141849446:role/saml/saml_cloud_developers` role.
* Make sure the `~/.aws/credentials` has *exact* lines for the `ecdev` profile.

```
[ecdev]
role_arn = arn:aws:iam::444732909647:role/cross_account/developers
source_profile = default
```

* If you need to use `gitops-control-plane`, generate `ecdev` AWS credentials with the `okta-awscli -o ecdev --profile=ecdev -f -s` command.
* Verify that the `ecdev` credentials work by running the `aws eks list-clusters --region=<<your_region>> --profile ecdev` command.
* If you need to use `esbench`, generate `elastic-dev` AWS credentials with the `okta-awscli -o default --profile=elastic-dev -f -s` command.
* Verify that the `elastic-dev` credentials work by running the `aws s3 ls --profile elastic-dev` command.
* If you want to run AWS third party tests on your development machine, create an own bucket with `aws s3 mb s3://<<your_bucket_name>> --region <<your_region>>`. You can the use it by passing the bucket name and region name via the `-Ds3.test.bucket` and `-Ds3.test.region` system properties.

