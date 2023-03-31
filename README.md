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

### Building and running locally with docker

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

If you would like to use a file system object store (see below if you would like to run with S3 instead), first create a permissive tmpfs directory `/tmp/objectstore` to use as a file system object store:

```shell
mkdir /tmp/objectstore ; chmod a+rw -R /tmp/objectstore
```

And then if you would like to run a cluster with 3 instances, with a separate Index and Search instance, you can run:

```shell
docker run --rm -d --name es01 --net elastic -p 9200:9200 -p 9300:9300 -e ES_JAVA_OPTS="-Xms1g -Xmx1g" -e node.name=es01 -e cluster.initial_master_nodes=es01,es02,es03 -e discovery.seed_hosts=es02,es03 -e node.roles='["master","index"]' -e xpack.security.enabled=false -e cluster.name=stateless -e stateless.enabled=true -e stateless.object_store.type=fs -e stateless.object_store.bucket=stateless -e path.repo=/objectstore -v /tmp/objectstore:/objectstore:z elasticsearch-serverless
docker run --rm -d --name es02 --net elastic -p 9202:9202 -p 9302:9302 -e ES_JAVA_OPTS="-Xms1g -Xmx1g" -e node.name=es02 -e cluster.initial_master_nodes=es01,es02,es03 -e discovery.seed_hosts=es01,es03 -e node.roles='["master","search"]' -e xpack.security.enabled=false -e cluster.name=stateless -e stateless.enabled=true -e stateless.object_store.type=fs -e stateless.object_store.bucket=stateless -e path.repo=/objectstore -v /tmp/objectstore:/objectstore:z elasticsearch-serverless
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
    "secrets": {
        "s3.client.test.access_key": "...",
        "s3.client.test.secret_key": "...",
        "s3.client.test.session_token": "..."
    }
}
```

Assuming that the `./secrets` directory is in your working directory, you can run Elasticsearch with:

```shell
docker run --rm -d --name es01 --net elastic -p 9200:9200 -p 9300:9300 -e ES_JAVA_OPTS="-Xms1g -Xmx1g" -e node.name=es01 -e cluster.initial_master_nodes=es01,es02,es03 -e discovery.seed_hosts=es02,es03 -e node.roles='["master","index"]' -e xpack.security.enabled=false -e cluster.name=stateless -e stateless.enabled=true -e stateless.object_store.type=s3 -e stateless.object_store.client=test -e stateless.object_store.bucket=... -v $(realpath ./secrets) elasticsearch-serverless
docker run --rm -d --name es02 --net elastic -p 9202:9202 -p 9302:9302 -e ES_JAVA_OPTS="-Xms1g -Xmx1g" -e node.name=es02 -e cluster.initial_master_nodes=es01,es02,es03 -e discovery.seed_hosts=es01,es03 -e node.roles='["master","search"]' -e xpack.security.enabled=false -e cluster.name=stateless -e stateless.enabled=true -e stateless.object_store.type=s3 -e stateless.object_store.client=test -e stateless.object_store.bucket=... -v $(realpath ./secrets) elasticsearch-serverless
docker run --rm -d --name es03 --net elastic -p 9203:9203 -p 9303:9303 -e ES_JAVA_OPTS="-Xms1g -Xmx1g" -e node.name=es03 -e cluster.initial_master_nodes=es01,es02,es03 -e discovery.seed_hosts=es01,es02 -e node.roles='["master"]' -e xpack.security.enabled=false -e cluster.name=stateless -e stateless.enabled=true -e stateless.object_store.type=s3 -e stateless.object_store.client=test -e stateless.object_store.bucket=... -v $(realpath ./secrets) elasticsearch-serverless
```

#### Setting up AWS development environment

* Install the `okta-awscli` tool according to the [docs](https://github.com/elastic/infra/blob/master/docs/aws/aws-user-access.md#apicli-access.
* Generate the AWS credentials with the `okta-awscli --profile=okta-elastic-dev -f -s` command. The credentials will be stored in the `~/.aws/credentials` file.
* Add the AWS profile to your environment: `export AWS_PROFILE=okta-elastic-dev`
* Verify the credentials by running the `aws s3 ls` command.
* Create an own bucket with `aws s3 mb s3://<<your_bucket_name>> --region <<your_region>>`
* You can run AWS third party tests on your development machine by specifying the bucket name and region name via the `-Ds3.test.bucket` and `-Ds3.test.region` system properties.
