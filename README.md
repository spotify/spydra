# Spydra

`Spydra` is "Hadoop Cluster as a Service" based on Google Cloud Dataproc and Google Cloud Storage.
The intention of Spydra is to enable the use of ephemeral Hadoop clusters while hiding
the complexity of cluster creation and lifecycle management from users.

Spydra supports submitting jobs Dataproc as well as submitting to existing on-premise scheduler.

*Spydra is a work in progress, and things may change at any time.*

## Spydra client

The purpose of the spydra client is to be a transparent submitter capable of submitting
batch processing jobs to both on-premise hadoop cluster and to Google Dataproc. Additionally it
offers some tools to handle logs and history of jobs executed on spydra-managed clusters.

**Spydra is currently under development, not feature complete and currently misses many convenience facilities.**

### Current CLI Functionality

Spydra supports multiple sub-commands:

* [`submit`](#submission) - submitting jobs to on-premise and dataproc
* [`dump-logs`](#retrieving-full-logs) - viewing logs
* [`dump-history`](#retrieving-full-history-data) - viewing history
* [`run-jhs`](#running-an-embedded-jobhistoryserver) - embedded history-server

#### Submission

```
$ spydra submit --help

usage: submit [options] [jobArgs]
    --clientid <arg>     client id, used as identifier in job history output
    --spydra-json <arg>   path to the spydra configuration json
    --jar <arg>          main jar path, overwrites the configured one if
                         set
    --jars <arg>         jar files to be shipped with the job, can occur
                         multiple times, overwrites the configured ones if
                         set
    --job-name <arg>     job name, used as dataproc job id
 -n,--dry-run            Do a dry run without executing anything
```

Only a few basic things can be supplied on the command line; a client-id (an arbitrary identifier of the client running Spydra),
the main and additional JAR files for the job, and arguments for the job. For any use-case requiring more details, the user needs to create
a JSON file and supply the path to that as a parameter.
All the command-line options will override the corresponding options in the JSON config.
Apart from all the command-line options and some general settings,
it can also transparently pass along parameters to the `gcloud` command for [cluster creation](https://cloud.google.com/sdk/gcloud/reference/dataproc/clusters/create) or [job submission](https://cloud.google.com/sdk/gcloud/reference/dataproc/jobs/submit/hadoop).

A job name can also be supplied. This will be sanitized and have a unique identifier attached to it, to then be used as the Dataproc job ID. This is mainly useful to find the job in the Google Cloud Console.

For details on the format of the JSON file see [this schema](/spydra/src/main/resources/spydra_config_schema.json) and
[these examples](spydra/src/main/resources/config_examples/).



#### Minimal submission example

Ohly command-line:
```
$ spydra submit --client-id simple-spydra-test --jar hadoop-mapreduce-examples.jar pi 8 100
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

#### Submission Gotchas

   * You can use `--` if you need to pass a parameter starting with dashes to your job, e.g. `submit --jar=jar ... -- -myParam`
   * Don't forget to specify `=` for arguments like `--jar=$jar`, otherwise the CLI parsing will break.
   * If the specified jar contains a Main-Class entry in it's manifest, specifying --mainclass will often lead to
     undesired behaviour, as the value of main-class will be passed as an argument to the application instead of
     invoking this class.
   * Not setting the default fs to GCS using the `fs.defaultFS` property can lead to crashes and undesired behavior
     as a lot of the frameworks use the default filesystem implementation instead of getting the correct filesystem
     for a given URI. It can also lead to the Crunch output committer working very slowly while copying all files from
     HDFS to GCS in a last non-distributed step.

### Dataproc user account

When creating a dataproc cluster, a service account name can be specified which is then used by the cluster when accessing external resources.
This account name can be specified in two ways:

   * Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the name of a json key file.
     The account name (`client_email`) in said file is then used.
   * Set `service-account` under cluster options in Spydra JSON config.

Note that in neither of these cases are any keys actually sent to Dataproc. In the former case (env-var pointing to a key file) it is normally
the same account as the one executing the spydra command. In the latter, the user running spydra needs to have permissions to run as the service account.
