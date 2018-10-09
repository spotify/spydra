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

package com.spotify.spydra.api;

import com.google.common.annotations.VisibleForTesting;
import com.spotify.spydra.api.gcloud.GcloudExecutor;
import com.spotify.spydra.api.model.Cluster;
import com.spotify.spydra.api.model.Job;
import com.spotify.spydra.metrics.Metrics;
import com.spotify.spydra.metrics.MetricsFactory;
import com.spotify.spydra.model.SpydraArgument;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DataprocApi {
  private final Metrics metrics;
  private final GcloudExecutor gcloud;
  private final Clock clock;

  public DataprocApi() {
    this(new GcloudExecutor(), MetricsFactory.getInstance(), Clock.systemUTC());
  }

  @VisibleForTesting
  DataprocApi(GcloudExecutor gcloud, Metrics metrics, Clock clock) {
    this.gcloud = gcloud;
    this.metrics = metrics;
    this.clock = clock;
  }

  public void dryRun(boolean dryRun) {
    gcloud.dryRun(dryRun);
  }

  public Optional<Cluster> createCluster(SpydraArgument arguments) throws IOException {
    boolean success = false;
    String zoneUri = null;
    try {
      Optional<Cluster> cluster = gcloud.createCluster(arguments.getCluster().getName(),
          arguments.getRegion(),
          arguments.getCluster().getOptions());
      success = cluster.isPresent();
      if (success) {
        zoneUri = cluster.get().config.gceClusterConfig.zoneUri;
      }
      return cluster;
    } finally {
      metrics.clusterCreation(arguments, zoneUri, success);
    }
  }

  public boolean deleteCluster(SpydraArgument arguments) throws IOException {
    Map<String, String> args = Collections.singletonMap(
        SpydraArgument.OPTION_PROJECT,
        arguments.getCluster().getOptions().get(SpydraArgument.OPTION_PROJECT));
    boolean success = false;
    try {
      success = gcloud.deleteCluster(arguments.getCluster().getName(), arguments.getRegion(), args);
    } finally {
      metrics.clusterDeletion(arguments, success);
    }
    return success;
  }

  public boolean submit(SpydraArgument arguments) throws IOException {
    boolean success = false;
    try {
      success = gcloud.submit(arguments.getJobType(),
          arguments.submit.pyFile,
          arguments.getRegion(),
          arguments.getSubmit().getOptions(),
          arguments.getSubmit().getJobArgs());
    } finally {
      metrics.jobSubmission(arguments, "dataproc", success);
    }
    return success;
  }

  public List<Cluster> listClusters(SpydraArgument arguments, Map<String, String> filters)
      throws IOException {
    String project = arguments.cluster.getOptions().get("project");
    String region = arguments.getRegion();
    return gcloud.listClusters(project, region, filters);
  }

  public Optional<Job> findJobToResume(SpydraArgument arguments)
      throws IOException {
    String project = arguments.cluster.getOptions().get("project");
    String region = arguments.getRegion();
    Map<String, String> labelItems = new HashMap<>();
    Map<String, String> labels = arguments.getSubmit().getLabels();

    String labelName = SpydraArgument.OPTIONS_DEDUPLICATING_LABEL;
    if (labels.containsKey(labelName)) {
      labelItems.put(
          String.format("labels.%s",labelName),
          labels.get(labelName));
    }
    List<Job> jobs = gcloud.listJobs(
        project,
        region,
        labelItems,
        Optional.of(1),
        Optional.of("~status.stateStartTime"));

    Optional<Duration> maxAge = arguments.deduplicationMaxAge();

    if (jobs.isEmpty()) {
      return Optional.empty();
    } else if (maxAge.isPresent()) {
      Job job = jobs.get(0);
      if (job.status.parseStateStartTime().isAfter(Instant.now(clock).minus(maxAge.get()))) {
        return Optional.of(job);
      } else {
        return Optional.<Job>empty();
      }
    } else {
      return Optional.of(jobs.get(0));
    }
  }

  public boolean waitJobForOutput(SpydraArgument arguments, String jobId) throws IOException {
    String region = arguments.getRegion();
    return gcloud.waitForOutput(region,jobId);
  }

}
