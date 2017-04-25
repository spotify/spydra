# Spydra (Beta)

Ephemeral Hadoop clusters using Google Compute Platform

## Description
`Spydra` is "Hadoop Cluster as a Service" implemented as a library utilizing [Google Cloud Dataproc](https://cloud.google.com/dataproc/) 
and [Google Cloud Storage](https://cloud.google.com/storage/). The intention of Spydra is to enable the use of ephemeral Hadoop clusters while hiding
the complexity of cluster lifecycle management and keeping troubleshooting simple. `Spydra` is designed to be integrated
as a `hadoop jar` replacement.

`Spydra` is part of Spotify's effort to migrate its data infrastructure to Google Compute Platform and is being used in
production. The principles and the design of `Spydra` are based on our experiences in scaling and maintaining our Hadoop
cluster to over 2500 nodes and over 100 PBs of capacity running about 20,000 independent jobs per day. 

`Spydra` supports submitting data processing jobs to Dataproc as well as to existing on-premise Hadoop infrastructure 
and is designed to ease the migration to and/or dual use of Google Cloud Platform and on-premise infrastructure.

`Spydra` is desined to be very configurable and allows the usage of all job types and configurations supported by the 
[gcloud dataproc clusters create](https://cloud.google.com/sdk/gcloud/reference/dataproc/jobs/submit/) and
[gcloud dataproc jobs submit](https://cloud.google.com/sdk/gcloud/reference/dataproc/jobs/submit/) commands.

### Development Status
`Spydra` is the rewrite of a concept that has been developed at Spotify for more than a year. The current version of
`Spydra` is in beta and is being actively developed and supported by our data infrastructure team at Spotify.

`Spydra` is in beta and things might change but we are aiming at not breaking the currently exposed APIs and configuration.

### How we use Spydra at Spotify
At Spotify, `Spydra` is being used for our on-going migration to Google Cloud Platform. It is being used for the 
submission of on-premise Hadoop jobs as well as Dataproc jobs and by that simplifies the switch from on-premise Hadoop
to Dataproc.

`Spydra` is being packaged in a [docker](https://www.docker.com/) image that is being used to deploy data
pipelines. This docker image includes Hadoop tools and configurations to be able to submit to our on-premise Hadoop
cluster as well as an installation of [gcloud](https://cloud.google.com/sdk/gcloud/) and other basic dependencies
required to execute Hadoop jobs in our environment. Pipelines are then being scheduled using [Styx](https://github.com/spotify/styx)
and orchestrated by [Luigi](https://github.com/spotify/luigi) which then invokes `Spydra` instead of `hadoop jar`.

### Design

`Spydra` is built as a wrapper around Google Cloud Dataproc and designed not to have any central component. It exposes
all functionality supported by Dataproc via its own configuration while adding some defaults. `Spydra` manages
clusters and submits jobs invoking the `gcloud dataproc` command. `Spydra` ensures that clusters are being deleted
by updating a heartbeat marker in the cluster's metadata and utilizes [initialization-actions](https://cloud.google.com/dataproc/docs/concepts/init-actions)
to set up a self-deletion script on the cluster to handle the deletion of the cluster in the event of client failures.

For submitting jobs to an existing on-premise Hadoop infrastructure, `Spydra` utilizes the `hadoop jar` command which is
required to be installed and configured in the environment. 

For Dataproc as well as on-premise submissions, `Spydra` will act similar to hadoop jar and print out driver output.

#### Credentials
`Spydra` is designed to ease the usage of Google Compute Platform credentials by utilizing 
[service accounts](https://cloud.google.com/compute/docs/access/service-accounts). The same credential that is being
used locally by `Spydra` to manage the cluster and submit jobs is by default being forwarded to the Hadoop cluster 
on Dataproc. By this means, access rights to resources need only to be provided to a single set of credentials.

#### Storing of execution data and logs
As to make job execution data available after a ephemeral cluster was shut down and to provide similar functionality to
the Hadoop MapReduce History Server, `Spydra` stores execution data and logs on Google Cloud Storage, grouping it by 
a user-defined client id. Typically client id is unique per job. The execution data and logs are then made available via 
`Spydra` commands. These allow spinning up a local MapReduce History Server to access execution data and logs
as well as dumping them.

#### Autoscaler
`Spydra` has an **experimental** autoscaler which can be executed on the cluster. It monitors the current resource
utilization on the cluster and scales the cluster according to a user defined utilization factor and maximum worker count
by adding [preemptible VMs](https://cloud.google.com/dataproc/docs/concepts/preemptible-vms). Note that the use of 
preemptible VMs might negatively impact performance as nodes might be shut down any time. Downscaling is optional
and highly experimental as it does not consider the current workload of the workers to be shut down and might trigger
job restarts.

The autoscaler is being installed on the cluster using an [initialization-action](https://cloud.google.com/dataproc/docs/concepts/init-actions).

#### Cluster pooling
`Spydra` has **experimental** support for cluster pooling withing a single Google Compute Platform project. The maximum
number of clusters to be used can be defined as well as their maximum lifetime. Upon job submission, a random cluster
is being chosen to submit the job to. When reaching their maximum lifetime, clusters are being deleted by the self-deletion
mechanism.

## Usage
### Installation
`Spydra` is not yet being packaged so you will need to build the executable yourself. We will provide a prepackaged
version in the near future.

### Environment Setup
To be able to use `Spydra` with Dataproc, a [Google Cloud Platform project](https://cloud.google.com/resource-manager/docs/creating-managing-projects)
with the right [APIs enable](https://support.google.com/cloud/answer/6158841?hl=en) is required. Additionally, a [service account](https://cloud.google.com/compute/docs/access/service-accounts)
with [project editor](https://cloud.google.com/compute/docs/access/iam) rights needs to be created and exported as json. Ensure that [gcloud](https://cloud.google.com/sdk/gcloud/) is installed and [authenticated using 
that service account](https://cloud.google.com/sdk/gcloud/reference/auth/). The environment variable
`GOOGLE_APPLICATION_CREDENTIALS` needs to point to the location of the service account json, see
 [Google Application Default Credentials](https://developers.google.com/identity/protocols/application-default-credentials).

For submitting to an on-premise Hadoop infrastructure, ensure that [hadoop jar](https://hadoop.apache.org/docs/r2.6.0/hadoop-project-dist/hadoop-common/CommandsManual.html#jar)
is installed and configured to submit to your cluster.

Java 8 needs to be installed.

### Spydra CLI

Spydra CLI supports multiple sub-commands:

* [`submit`](#submission) - submitting jobs to on-premise and Dataproc
* [`run-jhs`](#running-an-embedded-jobhistoryserver) - embedded history server
* [`dump-logs`](#retrieving-full-logs) - viewing logs
* [`dump-history`](#retrieving-full-history-data) - viewing history

#### Submit

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
of the client running Spydra), the main and additional JAR files for the job, and arguments for
the job. For any use-case requiring more details, the user needs to create a JSON file and supply
the path to that as a parameter. All the command-line options will override the corresponding
options in the JSON config. Apart from all the command-line options and some general settings,
it can also transparently pass along parameters to the `gcloud` command for
[cluster creation](https://cloud.google.com/sdk/gcloud/reference/dataproc/clusters/create) or
[job submission](https://cloud.google.com/sdk/gcloud/reference/dataproc/jobs/submit/hadoop).

A job name can also be supplied. This will be sanitized and have a unique identifier attached
to it, to then be used as the Dataproc job ID. This is mainly useful to find the job in
the Google Cloud Console.

##### The spydra-json argument
All properties that cannot be controlled via the few arguments of the submit command ca be set in the
configuration file supplied with --spydra-json. The configuration file follows the structure of the 
`cloud dataproc clusters create` and `cloud dataproc jubs submit` commands 
and allows to set all possible arguments of these commands. The basic structure looks as follows.

```$xslt
{
  "client_id": "hydra-test",                  # Hydra client id. Usually left out as set by the frameworks during runtime.
  "cluster_type": "dataproc",                 # Where to execute. Either dataproc or onpremise. Defaults to onpremise.
  "job_type": "hadoop",                       # Defaults to hadoop. For supported types see gcloud dataproc jobs submit --help
  "log_bucket": "hydra-test-logs",            # The bucket where Hadoop logs and history information are stored.
  "cluster": {                                # All cluster related configuration
    "options": {                              # Map supporting all options from the gcloud dataproc clusters create command
      "project": "hydra-test",                 
      "zone": "europe-west1-d",
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

Only command-line:
```
$ java -jar spydra/target/spydra-VERSION-jar-with-dependencies.jar submit --client-id simple-spydra-test --jar hadoop-mapreduce-examples.jar pi 8 100
```

JSON config:
```
$ cat example.json
{
  "client_id": "simple-spydra-test",
  "cluster_type": "dataproc",
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

##### Cluster Autoscaling (experimental)
Disclaimer: The usage of the autoscaler is experimental!

The Hydra autoscaler provides automatic sizing for Hydra clusters by adding enough preemptable worker nodes until a user supplied percentage of containers is running in parallel on the cluster. It enables cluster sizes to automatically adjust to growing resource needs over time and removes the need to come up with a good size when scheduling a job executed on Hydra.
The autoscaler has two modes, upscale only and downscale. Downscale will remove nodes when the cluster is not fully utilized. When doing so, it does currently not do this gracefully meaning that running containers might be killed possibly causing container retries or even application retries. Downscale should currently only be used for experimental purposes.
Enabling the Hydra autoscaler
To enable autoscaling add an autoscaler section similar to the one below to your Hydra configuration.

```$xslt
{
  "cluster:" {...},
  "submit:" {...},
  "auto_scaler": {
    "interval": "2",        # Execution interval of the autoscaler in minutes
    "max": "20",            # Maximum number of workers
    "factor": "0.3",        # Percentage of YARN containers that should be running at any point in time 0.0 to 1.0.
    "downscale": "false",   # Whether or not to downscale. Highly experimental! Please check our notes on downscaling!
  }
}
```

##### Cluster Pooling (experimental)
Disclaimer: The usage of the pooling is experimental!
The Hydra cluster pooling provides automatic pooling for Hydra clusters by selecting an existing cluster according to certain conditions.
Enabling pooling of clusters
To enable autoscaling add an autoscaler section similar to the one below to your Hydra configuration.

```$xslt
{
  "cluster:" {...},
  "submit:" {...},
  "pooling": {
    "limit": 2,     # limit of concurrent clusters
    "max_age": "20" # A java.time.Duration for the maximum age of a cluster
  }
}
```

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
     
#### Run-jhs
The run-jhs is designed for an interactive exploration of the job execution. This command spawns an embedded 
JobHistoryServer that can display all jobs executed using the same clientid. Familiarity with use of the JobHistoryServer 
from on-premise is assumed. The JHS is accessible on default port 19888.

The client id used when executing the job as well as the log bucket that was specified is required for running the command.

```java -jar spydra/target/spydra-VERSION-jar-with-dependencies.jar run-jhs --clientid=JOB_CLIENT_ID --log-bucket=LOG_BUCKET```

#### Dump-logs
The dump-logs command will dump logs for an application to stdout. Currently only full logs of the yarn application can be dumped - similarly to yarn logs when no specific container is specified. This is useful for processing/exploration with further tools in the shell.

The client id used when executing the job as well as the log bucket that was specified is required for running the command and
the Hadoop application id are required to run this command.
```java -jar spydra/target/spydra-VERSION-jar-with-dependencies.jar dump-logs --clientid=MY_CLIENT_ID --username=HADOOP_USERNAME --log-bucket=LOG_BUCKET --application=APPLICATION_ID```

#### Dump-history
The history files can be dumped as in regular Hadoop using the dump-history command.

The client id used when executing the job as well as the log bucket that was specified is required for running the command and
the Hadoop application id are required to run this command.
```java -jar spydra/target/spydra-VERSION-jar-with-dependencies.jar dump-history --clientid=MY_CLIENT_ID --log-bucket=LOG_BUCKET --application=APPLICATION_ID```

## Accessing Hadoop web interfaces on emphemeral clusters
[Dataprocxy](https://github.com/spotify/dataprocxy) can be used to open the web interfaces of the Hadoop daemons of
an emphemeral cluster as long as the cluster is running.

## Building

### Prerequisites
* Java JDK 8
* Maven 3.2.2
* A Google Compute Platform project with Dataproc enabled
* A Google Cloud Storage bucket for uploading init-actions
* A Google Cloud Storage bucket for storing integration test logs
* A [service account](https://cloud.google.com/compute/docs/access/service-accounts) with editor access to the project and bucket exported as json
* The environment variable `GOOGLE_APPLICATION_CREDENTIALS` pointing at the location of the service account json
* [gcloud](https://cloud.google.com/sdk/gcloud/) authenticated with the service account
* [gsutil](https://cloud.google.com/storage/docs/gsutil) authenticated with the service account

### Integration test configuration
In order to run integration tests, basic configuration needs to be provided during the build process. Create a spydra_conf.json
file similar to the one below and reference it during the maven invocation.

```$xslt
{
  "log_bucket": "YOUR_GCS_LOG_BUCKET",
  "cluster": {
    "options": {
      "project": "YOUR_PROJECT",
      "zone": "europe-west1-d"
    }
  }
}
```

### Building
Replace YOUR_INIT_ACTION_BUCKET with the bucket you created when setting up the prerquisites and YOUR_SPYDRA_CONF.JSON
with the path to the integration test configuration and execute the following maven command.

```mvn clean deploy -Dinit-action-uri=gs://YOUR_INIT_ACTION_BUCKET/spydra -Dtest-configuration-folder=YOUR_SPYDRA_CONF.JSON```

Executing the maven command above will create a spydra-VERSION-jar-with-dependencies.jar under spydra/target that packages `Spydra` and can be executed with `java -jar`.

## Contributing

This project adheres to the [Open Code of Conduct][code-of-conduct]. By participating,
you are expected to honor this code.

[code-of-conduct]: https://github.com/spotify/code-of-conduct/blob/master/code-of-conduct.md
