# Releasing Spydra

You need to have write access to [spotify/spydra](https://github.com/spotify/spydra) repository
for this to work.

Notice that Travis will run the integration tests and do the release for you,
so you do not have to run the release yourself locally. Nevertheless, you probably
want to run the integration tests to confirm your version will work, and also
use the Maven release plugin to update the versions.

1) Run the integration tests locally with whatever GCP project you have at hand with credentials.
   * Create the *integration-test-config.json* Spydra configuration as instructed in README, for example:
    ```json
    {
      "log_bucket": "varjo-testing-spydra-logs",
      "region": "europe-west1"
    }
    ```
   * Have credentials json file available that you want to use for running the integration tests.
   * Run the integration tests:
    ```bash
    env GOOGLE_APPLICATION_CREDENTIALS=/path/to/key/my-gcp-credentials-key.json mvn clean install \
    -Dinit-action-uri=gs://spydra-init-actions/spydra -Dtest-configuration-dir=/path/to/dir/with/test-config/
    ```

2) After you have confirmed the integration tests run successfully, run Maven prepare:
   ```bash
   env GOOGLE_APPLICATION_CREDENTIALS=/path/to/key/my-gcp-credentials-key.json mvn release:prepare \
   -Darguments="-DskipTests -Dinit-action-uri=gs://spydra-init-actions/spydra -Dtest-configuration-dir=/path/to/dir/with/test-config/"
   ```
   * Notice that we do not run the tests again with the second run, as we have skipTests defined.
     Release plugin requires us to use the -Darguments parameter wrapping to pass the parameters
     to the wrapped Maven call from inside release plugin.
   * Select the next version number and just get the release done.

3) Confirm that the actual release runs successfully in https://travis-ci.org/spotify/spydra
    Notice that Travis runs two parallel jobs for the two release commits that were made.

4) Confirm that the release tag got pushed correctly to https://github.com/spotify/spydra/releases