# Spydra (Beta)

[![License](https://img.shields.io/github/license/spotify/spydra.svg)](LICENSE)

Ephemeral Hadoop clusters using Google Compute Platform

## Description
`Spydra` is "Hadoop Cluster as a Service" implemented as a library utilizing [Google Cloud Dataproc](https://cloud.google.com/dataproc/) 
and [Google Cloud Storage](https://cloud.google.com/storage/). The intention of `Spydra` is to enable the use of ephemeral Hadoop clusters while hiding
the complexity of cluster lifecycle management and keeping troubleshooting simple. `Spydra` is designed to be integrated
as a `hadoop jar` replacement.

`Spydra` is part of Spotify's effort to migrate its data infrastructure to Google Compute Platform and is being used in
production. The principles and the design of `Spydra` are based on our experiences in scaling and maintaining our Hadoop
cluster to over 2500 nodes and over 100 PBs of capacity running about 20,000 independent jobs per day. 

`Spydra` supports submitting data processing jobs to Dataproc as well as to existing on-premise Hadoop infrastructure 
and is designed to ease the migration to and/or dual use of Google Cloud Platform and on-premise infrastructure.

`Spydra` is designed to be very configurable and allows the usage of all job types and configurations supported by the 
[gcloud dataproc clusters create](https://cloud.google.com/sdk/gcloud/reference/dataproc/jobs/submit/) and
[gcloud dataproc jobs submit](https://cloud.google.com/sdk/gcloud/reference/dataproc/jobs/submit/) commands.

### Development Status
`Spydra` is the rewrite of a concept that has been developed at Spotify for more than a year. The current version of
`Spydra` is in beta, used in production at Spotify, and actively developed and supported by our data infrastructure team.

`Spydra` is in beta and things might change but we are aiming at not breaking the currently exposed APIs and configuration.

### Spydra at Spotify
At Spotify, `Spydra` is being used for our on-going migration to Google Cloud Platform. It handles the 
submission of on-premise Hadoop jobs as well as Dataproc jobs, simplifying the switch from on-premise Hadoop
to Dataproc.

`Spydra` is packaged in a [docker](https://www.docker.com/) image that is used to deploy data
pipelines. This docker image includes Hadoop tools and configurations to be able to submit to our on-premise Hadoop
cluster as well as an installation of [gcloud](https://cloud.google.com/sdk/gcloud/) and other basic dependencies
required to execute Hadoop jobs in our environment. Pipelines are then scheduled using [Styx](https://github.com/spotify/styx)
and orchestrated by [Luigi](https://github.com/spotify/luigi) which then invokes `Spydra` instead of `hadoop jar`.

### Design

`Spydra` is built as a wrapper around Google Cloud Dataproc and designed not to have any central component. It exposes
all functionality supported by Dataproc via its own configuration while adding some defaults. `Spydra` manages
clusters and submits jobs invoking the `gcloud dataproc` command. `Spydra` ensures that clusters are eventually deleted
by updating a heartbeat marker in the cluster's metadata and utilizes [initialization-actions](https://cloud.google.com/dataproc/docs/concepts/init-actions)
to set up a self-deletion script on the cluster to handle the deletion of the cluster in the event of client failures.

For submitting jobs to an existing on-premise Hadoop infrastructure, `Spydra` utilizes the `hadoop jar` command which is
required to be installed and configured in the environment. 

For Dataproc as well as on-premise submissions, `Spydra` will act similar to hadoop jar and print out driver output.

#### Credentials
`Spydra` is designed to ease the usage of Google Compute Platform credentials by utilizing 
[service accounts](https://cloud.google.com/compute/docs/access/service-accounts). The same credential that is 
used locally by `Spydra` to manage the cluster and submit jobs, is also by default forwarded to the Hadoop cluster when
calling Dataproc. This means that access rights to resources need only be given to a single set of credentials.

#### Storing Execution Data and Logs
To make job execution data available after an ephemeral cluster was shut down, and to provide similar functionality to
the Hadoop MapReduce History Server, `Spydra` stores execution data and logs on Google Cloud Storage, grouping it by 
a user-defined client id. Typically client id is unique per job. The execution data and logs are then made available via 
`Spydra` commands. These allow spinning up a local MapReduce History Server to access execution data and logs
as well as dumping them.

#### Autoscaler
`Spydra` has an **experimental** autoscaler which can be executed on the cluster. It monitors the current resource
utilization on the cluster and scales the cluster according to a user defined utilization factor and maximum worker count
by adding [preemptible VMs](https://cloud.google.com/dataproc/docs/concepts/preemptible-vms). Note that the use of 
preemptible VMs might negatively impact performance as nodes might be shut down any time.

The autoscaler is being installed on the cluster using a Dataproc [initialization-action](https://cloud.google.com/dataproc/docs/concepts/init-actions).

#### Cluster Pooling
`Spydra` has **experimental** support for cluster pooling withing a single Google Compute Platform project. Cluster pooling
can be used to limit the resources used by the job submissions, and also limit the cluster initialization overhead.
The maximum number of clusters to be used can be defined as well as their maximum lifetime. Upon job submission, a random cluster
is chosen to submit the job into. When reaching their maximum lifetime, pooled clusters are being deleted by the self-deletion
mechanism.

## Usage
### Installation
There's a pre-built [Spydra on maven central](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.spotify.data.spydra%22%20a%3A%22spydra%22). This is built using the parameters from `.travis.yml`, the bucket `spydra-init-actions` is provided for by Spotify.

### Prerequisites
To be able to use Dataproc and on-premise Hadoop, a few things need to be set up before using `Spydra`.

* Java 8
* A [Google Cloud Platform project](https://cloud.google.com/resource-manager/docs/creating-managing-projects)
  with the right (Google Cloud Dataproc API) [APIs enabled](https://support.google.com/cloud/answer/6158841?hl=en)
* A [service account](https://cloud.google.com/compute/docs/access/service-accounts) with
  [project editor](https://cloud.google.com/compute/docs/access/iam) rights in your project. The service account can be specified in two ways:
  * A JSON key for the service account, and the environment variable [GOOGLE_APPLICATION_CREDENTIALS](https://developers.google.com/identity/protocols/application-default-credentials)
    needs to point to the location of this service account JSON key. This cannot be a user credential.
  * If the [GOOGLE_APPLICATION_CREDENTIALS](https://developers.google.com/identity/protocols/application-default-credentials)
    environment variable is not set, Spydra will attempt to use application default credentails. In a local development environment
    application default credentials can be obtained by authenticating with the command
    [gcloud auth application-default login](https://cloud.google.com/sdk/gcloud/reference/auth/application-default/login). When running
    on Google Compute Platform managed nodes, the application default credentials are provided by the default service account of the node.
* [gcloud](https://cloud.google.com/sdk/gcloud/) needs to be installed
* `gcloud` needs to be [authenticated using the service account](https://cloud.google.com/sdk/gcloud/reference/auth/)
* [hadoop jar](https://hadoop.apache.org/docs/r2.6.0/hadoop-project-dist/hadoop-common/CommandsManual.html#jar)
  needs to be installed and configured to submit to your cluster

### Spydra CLI

`Spydra` CLI supports multiple sub-commands:

* [`submit`](#submission) - submitting jobs to on-premise Hadoop and GCP Dataproc
* [`run-jhs`](#running-an-embedded-jobhistoryserver) - embedded history server
* [`dump-logs`](#retrieving-logs) - viewing logs
* [`dump-history`](#retrieving-history-data) - viewing history

#### Submission

```
$ java -jar spydra/target/spydra-VERSION-jar-with-dependencies.jar submit --help

usage: submit [options] [jobArgs]
    --clientid <arg>     client id, used as identifier in job history output
    --spydra-json <arg>  path to the spydra configuration json
    --jar <arg>          main jar path, overwrites the configured one if
                         set
    --jars <arg>         jar files to be shipped with the job, can occur
                         multiple times, overwrites the configured ones if
                         set
    --job-name <arg>     job name, used as dataproc job id
 -n,--dry-run            Do a dry run without executing anything
```

Only a few basic things can be supplied on the command line; a client-id (an arbitrary identifier
of the client running `Spydra`), the main and additional JAR files for the job, and arguments for
the job. For any use-case requiring more details, the user needs to create a JSON file and supply
the path to that as a parameter. All the command-line options will override the corresponding
options in the JSON config. Apart from all the command-line options and some general settings,
it can also transparently pass along parameters to the `gcloud` command for
[cluster creation](https://cloud.google.com/sdk/gcloud/reference/dataproc/clusters/create) or
[job submission](https://cloud.google.com/sdk/gcloud/reference/dataproc/jobs/submit/hadoop).

A job name can also be supplied. This will be sanitized and have a unique identifier attached
to it, which will then be used as the Dataproc job ID. This is useful in finding the job in
the Google Cloud Console.

##### The spydra-json argument
All properties that cannot be controlled via the few arguments of the *submit* command, can be set in the
configuration file supplied with the --spydra-json parameter. The configuration file follows the structure of the 
`cloud dataproc clusters create` and `cloud dataproc jubs submit` commands and allows to set all
the possible arguments for these commands. The basic structure looks as follows:

```json
{
  "client_id": "spydra-test",                 # Spydra client id. Usually left out as set by the frameworks during runtime.
  "cluster_type": "dataproc",                 # Where to execute. Either dataproc or onpremise. Defaults to onpremise.
  "job_type": "hadoop",                       # Defaults to hadoop. For supported types see gcloud dataproc jobs submit --help
  "log_bucket": "spydra-test-logs",           # The bucket where Hadoop logs and history information are stored.
  "region": "europe-west1",                   # The region in which the cluster is spun up
  "cluster": {                                # All cluster related configuration
    "options": {                              # Map supporting all options from the gcloud dataproc clusters create command
      "project": "spydra-test",
      "num-workers": "13",
      "worker-machine-type": "n1-standard-2", # The default machine type used by Dataproc is n1-standard-8.
      "master-machine-type": "n1-standard-4"
    }
  },
  "submit": {                                 # All configuration related to job submission
    "job_args": [                             # Job arguments. Usually left out as set by the frameworks during runtime.
      "pi",
      "2",
      "2"
    ],
    "options": {                              # Map supporting all options from the gcloud dataproc jobs submit [hadoop,spark,hive...] command
      "jar": "/path/my.jar"                   # Path of the job jar file. Usually left out as set by the frameworks during runtime.
    }
  }
}
```

For details on the format of the JSON file see
[this schema](/spydra/src/main/resources/spydra_config_schema.json) and
[these examples](spydra/src/main/resources/config_examples/).

##### Minimal Submission Example

Using only the command-line:
```
$ java -jar spydra/target/spydra-VERSION-jar-with-dependencies.jar submit --client-id simple-spydra-test --jar hadoop-mapreduce-examples.jar pi 8 100
```

JSON config:
```
$ cat examples.json
{
  "client_id": "simple-spydra-test",
  "cluster_type": "dataproc",
  "log_bucket": "spydra-test-logs",
  "region": "europe-west1",
  "cluster": {
    "options": {
      "project": "spydra-test"
    }
  },
  "submit": {
    "job_args": [
      "pi",
      "8",
      "100"
    ],
    "options": {
      "jar": "hadoop-mapreduce-examples.jar"
    }
  }
}
$ spydra submit --spydra-json example.json
```

##### Cluster Autoscaling (Experimental)
The `Spydra` autoscaler provides automatic sizing for `Spydra` clusters by adding enough preemptible worker
nodes until a user supplied percentage of containers is running in parallel on the cluster.
It enables cluster sizes to automatically adjust to growing resource needs over time and removes
the need to come up with a good size when scheduling a job executed on `Spydra`.
The autoscaler has two modes, upscale only and downscale.

Downscale will remove nodes when the cluster is not fully utilized. After
choosing to downscale, it will wait for the `downscale_timeout` to allow active
jobs to complete before terminating nodes. Note that though nodes may not have
active YARN containers running, active jobs may be storing intermediate
"shuffle" data on them. See [Dataproc Graceful
Downscale](https://cloud.google.com/dataproc/docs/concepts/scaling-clusters#using_graceful_decommissioning)
for more information.

To enable autoscaling, add an autoscaler section similar to the one below to your `Spydra` configuration.

```json
{
  "cluster:" {...},
  "submit:" {...},
  "auto_scaler": {
    "interval": "2",        # Execution interval of the autoscaler in minutes
    "max": "20",            # Maximum number of workers
    "factor": "0.3",        # Percentage of YARN containers that should be running at any point in time 0.0 to 1.0.
    "downscale": "false",    # Whether or not to downscale.
    # If downscale is enabled, how long in minutes to wait for active jobs to finish
    # before terminating nodes and potentially interrupting those jobs.
    # Note that the autoscaler will not be able to add nodes during this interval.
    "downscale_timeout": "10"
  }
}
```

##### Static Cluster Submission
If you prefer to manage your Dataproc clusters manually you still can use Spydra for job submission and just skip dynamic cluster creation part. The only change that is needed to be done to Spydra configurations is that you need to specify the name of the cluster you want to submit the job to. Here is an example:

```json
{
  "client_id": "simple-spydra-test",
  "cluster_type": "dataproc",
  "log_bucket": "spydra-test-logs",
  "submit": {
    "options": {
        "project": "spydra-test",
        "cluster": "NAME_OF_YOUR_CLUSTER"
    }
    "job_args": [
      "pi",
      "8",
      "100"
    ],
    "options": {
      "jar": "hadoop-mapreduce-examples.jar"
    }
  }
}
```
Also notice that `project` parameter is specified in `submit/options` section instead of `cluster/options` section.

##### Cluster Pooling (Experimental)
Disclaimer: The usage of the pooling is experimental!

The `Spydra` cluster pooling provides automatic pooling for `Spydra` clusters by selecting an existing
cluster according to certain conditions.

To enable cluster pooling add a pooling section similar to the one below to your `Spydra` configuration.

```json
{
  "cluster:" {...},
  "submit:" {...},
  "pooling": {
    "limit": 2,     # limit of concurrent clusters
    "max_age": "P1D"# A java.time.Duration for the maximum age of a cluster
  }
}
```

##### Duplicate submission avoidance
Spydra can be configured to avoid submitting duplicate jobs. This is useful for guarding against the 
Spydra process itself or its orchestrating process failing. Retrying in such situations would normally 
lead to duplicate jobs running, which will cause increased cost and, depending on the job implementation, 
may result in hard to debug failures or data corruption.

Duplicates are avoided by letting the client supply a job ID, in the form of a job label named `spydra-dedup-id`. 
If this ID is found on a previous job, Spydra will behave as follows:
   * If the previous job has succeeded, Spydra will print the output of the previous job and exit successfully. 
   * If the previous job has failed, Spydra will run the job, just as if no previous job was found.
   * If the previous job is in progress, Spydra will wait for the running job to finish, while printing the output 
     of that job. The exist status of Spydra will be the same as that of the running job. 

If multiple duplicate jobs exist, only the latest will be considered. 

It is possible to limit how far back in time spydra will look for duplicate jobs, using the `deduplication_max_age` 
parameter, specifying the maximum age of the duplicate job in seconds. 

Duplicate submission avoidance is not supported in conjunction with Static Cluster Submission or Cluster Pooling.

Example json file.

````json
{
  "client_id": "simple-spydra-deduplication-test",
  "cluster_type": "dataproc",
  "log_bucket": "spydra-test-logs",
  "region": "europe-west1",
  "deduplication_max_age":3600,
  "cluster": {
    "options": {
      "project": "spydra-test"
    }
  },
  "submit": {
    "job_args": [
      "pi",
      "8",
      "100"
    ],
    "options": {
      "jar": "hadoop-mapreduce-examples.jar",
      "labels":"spydra-dedup-id=1234"
    }
  }
}
````

##### Submission Gotchas
   * You can use `--` if you need to pass a parameter starting with dashes to your job,
     e.g. `submit --jar=jar ... -- -myParam`
   * Don't forget to specify `=` for arguments like `--jar=$jar`, otherwise the CLI parsing
     will break.
   * If the specified jar contains a Main-Class entry in it's manifest, specifying --mainclass
     will often lead to undesired behaviour, as the value of main-class will be passed as
     an argument to the application instead of invoking this class.
   * Not setting the default fs to GCS using the `fs.defaultFS` property can lead to crashes
     and undesired behavior as a lot of the frameworks use the default filesystem implementation
     instead of getting the correct filesystem for a given URI. It can also lead to the Crunch
     output committer working very slowly while copying all files from HDFS to GCS in a
     last non-distributed step.

#### Running an Embedded JobHistoryServer
The *run-jhs* is designed for an interactive exploration of the job execution. This command spawns an embedded 
JobHistoryServer that can display all jobs executed using the client id associated with your job submission.
Familiarity with the use of JobHistoryServer from on-premise Hadoop is assumed.
The JHS is accessible on default port 19888.

The client id used when executing the job, and the log bucket is required for running *run-jhs* command.

```java -jar spydra/target/spydra-VERSION-jar-with-dependencies.jar run-jhs --clientid=JOB_CLIENT_ID --log-bucket=LOG_BUCKET```

#### Retrieving Logs
The *dump-logs* command will dump logs for an application to stdout. Currently only full logs of
the YARN application can be dumped - similarly to YARN logs when no specific container is specified.
This is useful for processing/exploration with further tools in the shell.

The client id used when executing the job, the Hadoop application id, and the log bucket is required
for running *dump-logs* command.

```java -jar spydra/target/spydra-VERSION-jar-with-dependencies.jar dump-logs --clientid=MY_CLIENT_ID --username=HADOOP_USER_NAME --log-bucket=LOG_BUCKET --application=APPLICATION_ID```

#### Retrieving History Data
The history files can be dumped as in regular Hadoop using the *dump-history* command.

The client id used when executing the job, the Hadoop application id, and the log bucket is required
for running *dump-history* command.

```java -jar spydra/target/spydra-VERSION-jar-with-dependencies.jar dump-history --clientid=MY_CLIENT_ID --log-bucket=LOG_BUCKET --application=APPLICATION_ID```

## Accessing Hadoop Web Interfaces for Ephemeral Clusters
[Dataprocxy](https://github.com/spotify/dataprocxy) can be used to open the web interfaces of the Hadoop daemons of
an ephemeral cluster as long as the cluster is running.


## Building

### Prerequisites
* Java JDK 8
* Maven 3.2.2
* A Google Compute Platform project with Dataproc enabled
* A Google Cloud Storage bucket for uploading init-actions. Ensure that this bucket is readable with all credentials used with `Spydra`.
* A Google Cloud Storage bucket for storing integration test logs
* JSON key for a [service account](https://cloud.google.com/compute/docs/access/service-accounts)
  with editor access to the project and bucket.
* The environment variable `GOOGLE_APPLICATION_CREDENTIALS` pointing at the location of the service
  account JSON key
* [gcloud](https://cloud.google.com/sdk/gcloud/) authenticated with the service account
* [gsutil](https://cloud.google.com/storage/docs/gsutil) authenticated with the service account

### Integration Test Configuration

In order to run integration tests, basic configuration needs to be provided during the build process.
Create a file with name *integration-test-config.json* similar to the one below and reference
it during the maven invocation.

```json
{
  "log_bucket": "YOUR_GCS_LOG_BUCKET",
  "region": "europe-west1"
}
```

Replace the YOUR_GCS_LOG_BUCKET with a bucket you have in your GCP project for storing the logs.

The project will be taken from the service account credentials, you do not need to specify
the *project* parameter in *integration-test-config.json* (or elsewhere).

Notice that the file name must be exactly *integration-test-config.json* as that is what the
integration test will search for when it is run on the maven verify phase.

#### Integration testing with application default credentials

Due to a limitation in the
[GCS Connector library](https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage),
the integration tests do not work when using application default credentials, unless the tests are launched on
a Google Compute Platform managed node. Scripts for launching the tests in a Google Kubernetes Engine cluster
have been provided in [integration_test_k8s](./integration_test_k8s)

### Build, Test and Package

In the following command, replace YOUR_INIT_ACTION_BUCKET with the bucket you created
when setting up the prerequisites and YOUR_TEST_CONFIG_DIR with a directory name containing
the file *integration-test-config.json* you created in the previous step. YOUR_TEST_CONFIG_DIR
cannot be the same as the package root, so create a separate directory for this purpose.
Then execute the maven command:

```
mvn clean install -Dinit-action-uri=gs://YOUR_INIT_ACTION_BUCKET/spydra -Dtest-configuration-dir=YOUR_TEST_CONFIG_DIR
```

Executing the maven command above will run the integration tests, and create
a spydra-VERSION-jar-with-dependencies.jar under spydra/target that packages `Spydra`,
which can be executed with `java -jar`. Using `package` instead of `install` can be used 
to run just unit-tests and package Spydra.

If you want to copy the init-scripts into the defined init-action bucket, activate profile
`install-init-scripts`:

```
mvn clean install -Pinstall-init-scripts -Dinit-action-uri=gs://YOUR_INIT_ACTION_BUCKET/spydra -Dtest-configuration-dir=YOUR_TEST_CONFIG_DIR
```

Do not run Maven `deploy` step, as it will try to upload created packages into the Spotify owned
repositories, which will fail unless you have Spotify specific credentials.

## Communications

If you use Spydra and experience any issues, please create an issue under this Github project
in [here](https://github.com/spotify/spydra/issues/new).

You can also ask for help and talk to us on Spydra related issues in
[Spotify FOSS Slack](https://spotify-foss.slack.com) on channel
[#spydra](https://spotify-foss.slack.com/messages/C58L8GVS8/).

## Contributing

This project adheres to the [Open Code of Conduct][code-of-conduct]. By participating,
you are expected to honor this code.

[code-of-conduct]: https://github.com/spotify/code-of-conduct/blob/master/code-of-conduct.md
