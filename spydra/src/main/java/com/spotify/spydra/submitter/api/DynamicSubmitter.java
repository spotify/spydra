/*
 * Copyright 2017 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.spydra.submitter.api;

import static com.spotify.spydra.model.SpydraArgument.OPTION_CLUSTER;
import static com.spotify.spydra.model.SpydraArgument.OPTION_METADATA;
import static com.spotify.spydra.model.SpydraArgument.OPTION_PROJECT;
import static com.spotify.spydra.model.SpydraArgument.OPTION_ZONE;

import com.spotify.spydra.api.DataprocAPI;
import com.spotify.spydra.api.model.Cluster;
import com.spotify.spydra.metrics.Metrics;
import com.spotify.spydra.metrics.MetricsFactory;
import com.spotify.spydra.model.SpydraArgument;
import com.spotify.spydra.util.GcpUtils;
import java.io.IOException;
import java.net.URI;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamicSubmitter extends Submitter {
  private static final Logger LOGGER = LoggerFactory.getLogger(DynamicSubmitter.class);

  private final Metrics metrics = MetricsFactory.getInstance();

  private final static String DEFAULT_CLUSTER_PREFIX = "spydra";
  private final static String METADATA_KEY = "heartbeat";
  private final static String COLLECTOR_INTERVAL_KEY = "collector-timeout";

  private final static String SPYDRA_CLUSTER_LABEL = "spydra-cluster";

  private final DataprocAPI dataprocAPI;
  private final GcpUtils gcpUtils;

  public DynamicSubmitter() {
    this(new DataprocAPI(), new GcpUtils());
  }

  public DynamicSubmitter(String account) {
    this(new DataprocAPI(account), new GcpUtils());
  }

  public DynamicSubmitter(DataprocAPI dataprocAPI, GcpUtils gcpUtils) {
    this.dataprocAPI = dataprocAPI;
    this.gcpUtils = gcpUtils;

  }

  class Heartbeater implements Runnable {
    private final SpydraArgument arguments;
    private final DataprocAPI dataprocAPI;

    public Heartbeater(SpydraArgument arguments, DataprocAPI dataprocAPI) {
      this.arguments = arguments;
      this.dataprocAPI = dataprocAPI;
    }

    @Override
    public void run() {
      while (!Thread.interrupted()) {
        try {
          if (!arguments.isDryRun()) {
            dataprocAPI.updateMasterMetadata(arguments, METADATA_KEY, nowUtc());
          } else {
            LOGGER.info("Dry-run: Not updating metadata");
          }
          Thread.sleep(TimeUnit.SECONDS.toMillis(this.arguments.getHeartbeatIntervalSeconds()));
        } catch (IOException e) {
          LOGGER.error("Failed to update liveness metadata", e);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  static String nowUtc() {
    // TODO Should really use a standard format
    ZonedDateTime utc = ZonedDateTime.now(ZoneOffset.UTC);
    return utc.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"));
  }

  @Override
  public boolean executeJob(SpydraArgument argument) {

    dataprocAPI.dryRun(argument.isDryRun());

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      SpydraArgument.addOption(argument.getCluster().getOptions(),
          OPTION_METADATA, METADATA_KEY + "=" + nowUtc());
      SpydraArgument.addOption(argument.getCluster().getOptions(),
          OPTION_METADATA, COLLECTOR_INTERVAL_KEY + "=" + argument.getCollectorTimeoutMinutes());
      if (!acquireCluster(argument, dataprocAPI)) {
        return false;
      }
      executor.submit(new Heartbeater(argument, dataprocAPI));
      return super.executeJob(argument);
    } catch (IOException e) {
      LOGGER.error("Failed to create cluster", e);
      metrics.fatalError(argument, e);
      return false;
    } finally {
      executor.shutdownNow();
      try {
        executor.awaitTermination(argument.getHeartbeatIntervalSeconds(), TimeUnit.SECONDS);
      } catch (InterruptedException e) {
      }
      try {
        releaseCluster(argument, dataprocAPI);
      } catch (IOException e) {
        LOGGER.warn("Failed to release cluster", e);
      }
    }
  }

  public boolean acquireCluster(SpydraArgument arguments, DataprocAPI dataprocAPI)
      throws IOException {
    return createNewCluster(arguments, dataprocAPI);
  }

  boolean createNewCluster(SpydraArgument arguments, DataprocAPI dataprocAPI)
      throws IOException {
    String name = generateName();
    arguments.getCluster().setName(name);
    SpydraArgument createArguments;
    if (arguments.autoScaler.isPresent()) {
      createArguments = configureAutoScaler(arguments);
    } else {
      createArguments = arguments;
    }

    arguments.addOption(createArguments.cluster.options, SpydraArgument.OPTION_LABELS, SPYDRA_CLUSTER_LABEL + "=1");

    randomizeZoneIfAbsent(createArguments);

    Optional<Cluster> cluster = dataprocAPI.createCluster(createArguments);
    if (cluster.isPresent()) {
      mutateForCluster(arguments, name, cluster.get().config.gceClusterConfig.zoneUri);
    }
    return cluster.isPresent();
  }

  void randomizeZoneIfAbsent(SpydraArgument createArguments) {
    if (createArguments.defaultZones.size() > 0) {
      createArguments.cluster.getOptions().computeIfAbsent(SpydraArgument.OPTION_ZONE,
          (e) -> createArguments.defaultZones
              .get(new Random().nextInt(createArguments.defaultZones.size())));
    }
  }

  protected void mutateForCluster(SpydraArgument arguments, String name, String zone) {
    arguments.getCluster().setName(name);
    arguments.getCluster().getOptions().put(OPTION_ZONE, zone);
    arguments.getSubmit().getOptions().put(OPTION_CLUSTER, name);
    arguments.getSubmit().getOptions().put(OPTION_PROJECT,
        arguments.getCluster().getOptions().get(OPTION_PROJECT));
  }

  private SpydraArgument configureAutoScaler(SpydraArgument arguments) {
    SpydraArgument metadataArgument = new SpydraArgument();
    ArrayList<String> list = new ArrayList<>();
    list.add("autoscaler-interval=" + arguments.getAutoScaler().getInterval());
    list.add("autoscaler-max=" + arguments.getAutoScaler().getMax());
    list.add("autoscaler-factor=" + arguments.getAutoScaler().getFactor());
    list.add("autoscaler-mode="
        + (arguments.getAutoScaler().getDownscale() ? "downscale" : "upscale"));
    list.add("autoscaler-downscale-timeout=" + (arguments.getAutoScaler().getDownscale() ?
        // Statically configure a no-op value for auto_scaler.sh:
        arguments.getAutoScaler().getDownscaleTimeout() : 0));
    metadataArgument.cluster.getOptions()
        .put(SpydraArgument.OPTION_METADATA, StringUtils.join(list, ","));
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
    FileSystem fs = gcpUtils.fileSystemForUri(uri);
    LOGGER.info("Waiting for history files to be moved to its final location");
    while (fs.getContentSummary(new Path(uri)).getFileCount() != 0) {
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
   * @param arguments   describing the cluster to be released
   * @param dataprocAPI dataprocAPI implementation to use to wait for history and release the cluster
   * @return whether it was successfully released
   */
  public boolean releaseCluster(SpydraArgument arguments, DataprocAPI dataprocAPI) throws IOException {
    try {
      waitForHistoryToBeMoved(arguments);
    } finally {
      return dataprocAPI.deleteCluster(arguments);
    }
  }
}
