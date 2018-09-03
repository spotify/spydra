/*-
 * -\-\-
 * Spydra
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.spydra.submitter.api;

import static com.spotify.spydra.model.SpydraArgument.OPTIONS_DEDUPLICATING_LABEL;
import static com.spotify.spydra.model.SpydraArgument.OPTION_CLUSTER;
import static com.spotify.spydra.model.SpydraArgument.OPTION_PROJECT;
import static com.spotify.spydra.model.SpydraArgument.OPTION_ZONE;

import com.spotify.spydra.api.DataprocApi;
import com.spotify.spydra.api.model.Cluster;
import com.spotify.spydra.api.model.Job;
import com.spotify.spydra.metrics.Metrics;
import com.spotify.spydra.metrics.MetricsFactory;
import com.spotify.spydra.model.SpydraArgument;
import com.spotify.spydra.submitter.executor.ExecutorFactory;
import com.spotify.spydra.util.GcpUtils;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamicSubmitter extends Submitter {

  private static final Logger LOGGER = LoggerFactory.getLogger(DynamicSubmitter.class);

  private final Metrics metrics = MetricsFactory.getInstance();

  private static final String DEFAULT_CLUSTER_PREFIX = "spydra";

  public static final String SPYDRA_CLUSTER_LABEL = "spydra-cluster";

  private final DataprocApi dataprocApi;
  private final GcpUtils gcpUtils;

  public DynamicSubmitter() {
    this(new DataprocApi(), new GcpUtils());
  }

  public DynamicSubmitter(DataprocApi dataprocApi, GcpUtils gcpUtils) {
    this.dataprocApi = dataprocApi;
    this.gcpUtils = gcpUtils;

  }

  @Override
  public boolean executeJob(SpydraArgument argument) {

    dataprocApi.dryRun(argument.isDryRun());
    try {
      if (argument.submit.getLabels().containsKey(OPTIONS_DEDUPLICATING_LABEL)) {
        Optional<Job> maybeJob = dataprocApi.findJobToResume(argument);
        if (maybeJob.isPresent()) {
          Job job = maybeJob.get();
          if (job.status.isDone() || job.status.isInProggress()) {
            LOGGER.info(String.format(
                "Attempted to submit duplicate of Job[%s]. "
                  + "Will wait for original job instead of submitting new.", job.reference.jobId));
            return dataprocApi.waitJobForOutput(argument, job.reference.jobId);
          }
        }
      }
    } catch (IOException e) {
      LOGGER.error("Failed to list jobs", e);
      return false;
    }

    try {
      if (!acquireCluster(argument, dataprocApi)) {
        return false;
      }
      return executeJob(new ExecutorFactory(() -> dataprocApi), argument);
    } catch (Exception e) {
      LOGGER.error("Failed to create cluster", e);
      metrics.fatalError(argument, e);
      return false;
    } finally {
      try {
        releaseCluster(argument, dataprocApi);
      } catch (IOException e) {
        LOGGER.warn("Failed to release cluster", e);
      }
    }
  }

  public boolean acquireCluster(SpydraArgument arguments, DataprocApi dataprocApi)
      throws IOException {

    Optional<Cluster> newCluster = createNewCluster(arguments, dataprocApi);

    newCluster.ifPresent(cluster ->
                             setTargetCluster(arguments, arguments.getCluster().getName(),
                                              cluster.config.gceClusterConfig.zoneUri));

    return newCluster.isPresent();
  }

  Optional<Cluster> createNewCluster(SpydraArgument arguments, DataprocApi dataprocApi)
      throws IOException {
    return createNewCluster(arguments, dataprocApi, DynamicSubmitter::generateName);
  }

  Optional<Cluster> createNewCluster(
      SpydraArgument arguments, DataprocApi dataprocApi,
      Supplier<String> nameGenerator)
      throws IOException {
    arguments.getCluster().setName(nameGenerator.get());

    SpydraArgument createArguments;
    if (arguments.autoScaler.isPresent()) {
      createArguments = configureAutoScaler(arguments);
    } else {
      createArguments = arguments;
    }

    arguments.addOption(createArguments.cluster.options, SpydraArgument.OPTION_CLUSTER_LABELS,
                        SPYDRA_CLUSTER_LABEL + "=1");

    Optional<Cluster> cluster = dataprocApi.createCluster(createArguments);

    return cluster;
  }

  protected void setTargetCluster(SpydraArgument arguments, String name, String zone) {
    arguments.getCluster().setName(name);
    arguments.getCluster().getOptions().put(OPTION_ZONE, zone);
    arguments.getSubmit().getOptions().put(OPTION_CLUSTER, name);
    arguments.getSubmit().getOptions().put(
        OPTION_PROJECT,
        arguments.getCluster().getOptions().get(OPTION_PROJECT));
  }

  private SpydraArgument configureAutoScaler(SpydraArgument arguments) {
    final SpydraArgument metadataArgument = new SpydraArgument();
    List<String> list = new ArrayList<>();
    list.add("autoscaler-interval=" + arguments.getAutoScaler().getInterval());
    list.add("autoscaler-max=" + arguments.getAutoScaler().getMax());
    list.add("autoscaler-factor=" + arguments.getAutoScaler().getFactor());
    list.add("autoscaler-mode="
             + (arguments.getAutoScaler().getDownscale() ? "downscale" : "upscale"));
    list.add("autoscaler-downscale-timeout="
             + (arguments.getAutoScaler().getDownscale()
                // Statically configure a no-op value for auto_scaler.sh:
                ? arguments.getAutoScaler().getDownscaleTimeout()
                : 0));
    metadataArgument.cluster.getOptions()
        .put(SpydraArgument.OPTION_METADATA, String.join(",", list));
    return SpydraArgument.merge(arguments, metadataArgument);
  }

  public static String generateName() {
    return String.format("%s-%s", DEFAULT_CLUSTER_PREFIX, UUID.randomUUID().toString());
  }

  private void waitForHistoryToBeMoved(SpydraArgument arguments) throws IOException {
    if (arguments.isDryRun()) {
      return;
    } //Skip this entirely on dry-run

    int timeoutSeconds = arguments.getHistoryTimeout();
    if (timeoutSeconds <= 0) {
      return;
    } // Do not wait if timeout non-positive
    long start = System.currentTimeMillis();
    String path = arguments.clusterProperties()
        .getProperty("mapred:mapreduce.jobhistory.intermediate-done-dir");
    URI uri = URI.create(path);
    final String bucketName = uri.getHost();
    String directory = uri.getPath();
    if (directory != null && directory.length() > 0) {
      directory = directory.substring(1, directory.length()); //remove leading slash from the path
    }
    LOGGER.info("Waiting for history files to be moved to its final location");
    gcpUtils.configureStorageFromEnvironment();
    while (gcpUtils.getCount(bucketName, directory + "/") <= 1) { //directory itself counts as one
      LOGGER.info("Not yet moved files were encountered. Sleeping 1 second.");
      try {
        long now = System.currentTimeMillis();
        if (now - start > TimeUnit.SECONDS.toMillis(timeoutSeconds)) {
          throw new IOException("Timed out waiting for the history to be moved");
        }
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOGGER.debug("History move wait was interrupted", e);
        break;
      }
    }
  }

  /**
   * Release a cluster.
   *
   * @param arguments   describing the cluster to be released
   * @param dataprocApi dataprocApi implementation to use to wait for history and release
   *                    the cluster
   * @return whether it was successfully released
   */
  public boolean releaseCluster(SpydraArgument arguments, DataprocApi dataprocApi)
      throws IOException {
    try {
      waitForHistoryToBeMoved(arguments);
    } finally {
      return dataprocApi.deleteCluster(arguments);
    }
  }
}
