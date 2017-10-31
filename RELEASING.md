# Releasing Spydra

You need to have write access to [spotify/spydra](https://github.com/spotify/spydra) repository
for this to work.

Notice that Travis will run the integration tests and do the release for you,
so you do not have to run the release yourself locally. Nevertheless, you probably
want to run the integration tests to confirm your version will work, and also
use the Maven release plugin to update the versions.

1) Run the integration tests locally with whatever GCP project you have at hand with credentials.
   * Have credentials json file available that you want to use for running the integration tests.
   * Run the integration tests:
    ```bash
    env GOOGLE_APPLICATION_CREDENTIALS=/path/to/key/my-gcp-credentials-key.json mvn clean verify
    ```

2) After you have confirmed the integration tests run successfully, run Maven prepare:
   ```bash
   env GOOGLE_APPLICATION_CREDENTIALS=/path/to/key/my-gcp-credentials-key.json mvn release:prepare
   ```
   * Release plugin requires us to use the -Darguments parameter wrapping to pass the parameters
     to the wrapped Maven call from inside release plugin. If you customized the configuration for
     integration tests specify a suitable `-Darguments`:
   * Select the next version number and just get the release done.

3) The maven release plugin is configured not to push automatically, an automatic push would result
   in a build only on the tip. Push `HEAD^1`, `HEAD` and tags individually and confirm that the
   actual release runs successfully in https://travis-ci.org/spotify/spydra. Notice that Travis
   runs two parallel jobs for the two release commits that were made.

4) Confirm that the release tag got pushed correctly to https://github.com/spotify/spydra/releases

# Changing the default configurations

Spydra comes with 2 options configured by default: `test-configuration-dir` and `init-action-uri`.
If you wish to change these, configure them as `-D` parameters. Remember that for the maven release
plugin you need to wrap these in a `-Darguments="-D...=... -D..."`
* `test-configuration-dir` takes a path, if not absolute it is resolved relative to `spydra/pom.xml`
  If you want to customize the configuration for the integration tests create a
  *integration-test-config.json* Spydra configuration as instructed in README, for example:
  ```json
  {
    "log_bucket": "varjo-testing-spydra-logs",
    "region": "europe-west1"
  }
  ```
* `init-action-uri` takes a uri to which a `/${project.version}` is appended.

