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

package com.spotify.spydra.historytools;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapreduce.jobhistory.HistoryViewer;
import org.apache.hadoop.mapreduce.v2.hs.JobHistoryServer;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.logaggregation.LogCLIHelpers;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HistoryLogUtils {

  private static final Logger logger = LoggerFactory.getLogger(HistoryLogUtils.class);
  public static final String DEFAULT_USER_NAME = "root";
  public static final String HISTORY_LOG_CONFIG_NAME = "history.xml";

  public static final String SPYDRA_HISTORY_CLIENT_ID_PROPERTY = "spydra.job.history.client_id";
  public static final String SPYDRA_HISTORY_USERNAME_PROPERTY = "spydra.job.history.username";
  public static final String SPYDRA_HISTORY_BUCKET_PROPERTY = "spydra.job.history.bucket";
  // for cases when we don't actually need a username, such as history operations
  private static final String DUMMY_USER_NAME = "dummy";

  /**
   * Creates a specialized hadoop configuration for spydra. This configuration is
   * special in the sense that it configures hadoop tooling to be able to access GCS
   * for logs, history and is able to run a read-only job-history server (not moving
   * or deleting logs or history files). This configuration is dependent on client
   * and username due to how this information is stored in GCS.
   *
   * @param clientId client id to generate configuration for
   * @param username username to generate configuration for
   * @param bucket   name of the bucket storing logs and history information
   */
  public static Configuration generateHadoopConfig(String clientId, String username,
      String bucket) {
    // We want minimalistic and clean options that are unlikely to collide with anything,
    // that's why not loading defaults
    Configuration cfg = new Configuration(false);
    cfg.addResource(HISTORY_LOG_CONFIG_NAME);
    cfg.reloadConfiguration();
    cfg.set(SPYDRA_HISTORY_CLIENT_ID_PROPERTY, clientId);
    cfg.set(SPYDRA_HISTORY_USERNAME_PROPERTY, username);
    cfg.set(SPYDRA_HISTORY_BUCKET_PROPERTY, bucket);

    if (logger.isDebugEnabled()) {
      logger.debug("Dumping generated config to be applied for log/history tools");
      logger.debug(Joiner.on("\n").join(cfg.iterator()));
    }

    return cfg;
  }

  /**
   * Convenience version of @{see LogReader#generateHadoopConfig} for operations where a user
   * is not required.
   */
  public static Configuration generateHadoopConfig(String clientId, String bucket) {
    return generateHadoopConfig(clientId, DUMMY_USER_NAME, bucket);
  }

  /**
   * Dumps the full job logs for a particular application to stdout
   *
   * @param applicationId application to dump logs for
   */
  public static void dumpFullLogs(Configuration cfg, ApplicationId applicationId) {
    LogCLIHelpers logCLIHelpers = new LogCLIHelpers();
    // TODO: Add the proper base dir settings etc...

    logCLIHelpers.setConf(cfg);
    try {
      logCLIHelpers.dumpAllContainersLogs(applicationId, cfg.get(SPYDRA_HISTORY_USERNAME_PROPERTY), System.out);
    } catch (IOException e) {
      logger.error("Failed dumping log files for application " + applicationId.toString(), e);
    }
  }

  /**
   * Dumps the full job history information to stdout.
   *
   * @param applicationId application to dump history for
   */
  public static void dumpFullHistory(Configuration cfg, ApplicationId applicationId) {
    try {
      // TODO: This might be a problem if we have intermediate and done dirs in different filesystems
      FileSystem fs = FileSystem.get(new URI(cfg.get(JHAdminConfig.MR_HISTORY_DONE_DIR)), cfg);

      // TODO: Seems to hang if there is no listing??
      findHistoryFilePath(fs, cfg.get(JHAdminConfig.MR_HISTORY_DONE_DIR), applicationId)
          .ifPresent(x -> {
            try {
              logger.info("Starting HistoryViewer");
              new HistoryViewer(x, cfg, true).print();
            } catch (IOException e) {
              logger.error("Failed running HistoryViewer to dump history", e);
            }
          });
    } catch (IOException e) {
      logger.error("Failed instantiating filesystem", e);
    } catch (URISyntaxException e) {
      logger.error("history location is not a valid URI", e);
    }
  }

  /**
   * Tries to locate a mapreduce job history file for some client id and application
   *
   * @return Path of the located jhist file
   * @throws IOException        Failure to initialize filesystem to access history file
   * @throws URISyntaxException Location specified for history path prefix is not a valid URI
   */
  public static Optional<String> findHistoryFilePath(FileSystem fs, String historyDirPrefix,
      ApplicationId applicationId) throws IOException, URISyntaxException {
    Path jhistPathPattern = new Path(historyDirPrefix);
    return findHistoryFilePath(new RemoteIteratorAdaptor<>(fs.listFiles(jhistPathPattern, true)), applicationId);
  }

  public static Optional<String> findHistoryFilePath(Iterator<LocatedFileStatus> listing,
      ApplicationId applicationId) {

    JobID jobId = new JobID(String.valueOf(applicationId.getClusterTimestamp()), applicationId.getId());

    List<LocatedFileStatus> jhistFiles = Lists.newArrayList();
    // maybe this could work more nicely with some recursive glob and a filter
    try {
      jhistFiles = StreamSupport
          .stream(Spliterators.spliteratorUnknownSize(listing, Spliterator.NONNULL), false)
          .filter(fstatus -> fstatus.getPath().toString()
              .matches(".*" + jobId.toString() + ".*.jhist"))
          .collect(Collectors.toList());
    } catch (RemoteIteratorAdaptor.WrappedRemoteIteratorException wrie) {
      // We can't really do overly much at this point, as this is an error from the
      // underlying hadoop filesystem implementation. But we want to at least log this
      // separately from other conditions.
      logger.error("Retrieving remote listing failed", wrie);
    }

    if (jhistFiles.size() < 1) {
      logger.error("Could not locate a history file for parameters");
      return Optional.empty();
    } else if (jhistFiles.size() > 1) {
      logger.error("Found two or more matching files, will dump first");
    }

    return jhistFiles.stream()
        .findFirst()
        .map(x -> x.getPath().toString());
  }

  /**
   * Starts a minimal JobHistoryServer
   */
  public static void startJHS(Configuration cfg) {
    try {
      JobHistoryServer jobHistoryServer = new JobHistoryServer();
      jobHistoryServer.init(cfg);
      logger.info(String.format("Starting JobHistoryServer on: http://%s",
          cfg.get(JHAdminConfig.MR_HISTORY_WEBAPP_ADDRESS)));
      jobHistoryServer.start();
    } catch (Exception e) {
      logger.error("Error starting JobHistoryServer", e);
      System.exit(1);
    }
  }

  public static void main(String[] args) {
    String clientId = args[0];
    String appIdStr = args[1];
    String bucket = args[2];

    ApplicationId appId = ConverterUtils.toApplicationId(appIdStr);
    Configuration cfg = generateHadoopConfig(clientId, DEFAULT_USER_NAME, bucket);

    logger.info("Dumping log files");
    dumpFullLogs(cfg, appId);
    logger.info("Dumping full history information");
    dumpFullHistory(cfg, appId);
    logger.info("Starting local JHS");
    startJHS(cfg);
  }

}
