# Elasticsearch Stateless

This repository is home for code specific to the "stateless" distribution of Elasticsearch.

## Getting started

### Cloning the repository

Stateless Elasticsearch is currently build with "core" [Elasticsearch](https://github.com/elastic/elasticsearch) as a
baseline, with additional modules and changes as necessary. Elasticsearch is brought in as a source build dependency via
a [Git submodule](https://www.git-scm.com/book/en/v2/Git-Tools-Submodules). As such, interacting with the code in this
repository is a bit different, depending on whether you are modifying stateless code, or core Elasticsearch code. To 
start, when doing a clone, you'll need to instruct Git to also clone any submodules, like so:

```shell
git clone --recurse-submodules git@github.com:elastic/elasticsearch-stateless.git
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

The stateless project shares all the same IDE integration as core Elasticsearch so in terms of tooling and IntelliJ
integration, please refer to the [Elasticsearch contributor guide](/elasticsearch/CONTRIBUTING.md).

### Building and running locally

Stateless Elasticsearch can be built using the command:

```shell
./gradlew buildDockerImage
```

An easy way to test the docker image locally is by deploying a 3 nodes cluster like this:

```shell
# Create the Docker network that is used by the three instances
docker network create elastic
# Start the instances
docker run --rm -d --name es01 --net elastic -p 9200:9200 -p 9300:9300 -e ES_JAVA_OPTS="-Xms1g -Xmx1g" -e xpack.security.enabled=false -e cluster.name=stateless -e node.name=es01 -e cluster.initial_master_nodes=es01,es02,es03 -e discovery.seed_hosts=es02,es03 -e node.roles='["master","index"]' -e stateless.enabled=true elasticsearch-stateless:x86_64
docker run --rm -d --name es02 --net elastic -p 9202:9202 -p 9302:9302 -e ES_JAVA_OPTS="-Xms1g -Xmx1g" -e xpack.security.enabled=false -e cluster.name=stateless -e node.name=es02 -e cluster.initial_master_nodes=es01,es02,es03 -e discovery.seed_hosts=es01,es03 -e node.roles='["master","search"]' -e stateless.enabled=true elasticsearch-stateless:x86_64
docker run --rm -d --name es03 --net elastic -p 9203:9203 -p 9303:9303 -e ES_JAVA_OPTS="-Xms1g -Xmx1g" -e xpack.security.enabled=false -e cluster.name=stateless -e node.name=es03 -e cluster.initial_master_nodes=es01,es02,es03 -e discovery.seed_hosts=es01,es02 -e node.roles='["master"]' -e stateless.enabled=true elasticsearch-stateless:x86_64
```

You can access the logs of an instance using:
```shell
docker logs -f es01
```

You can stop an instance using:
```shell
docker container stop es01
```