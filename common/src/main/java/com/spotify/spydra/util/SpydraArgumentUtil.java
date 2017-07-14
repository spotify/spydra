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

package com.spotify.spydra.util;

import com.spotify.spydra.model.JsonHelper;
import com.spotify.spydra.model.SpydraArgument;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;

import static com.spotify.spydra.model.ClusterType.DATAPROC;
import static com.spotify.spydra.model.SpydraArgument.OPTION_SERVICE_ACCOUNT;

public class SpydraArgumentUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(SpydraArgumentUtil.class);

  public static final String BASE_CONFIGURATION_FILE_NAME = "defaults.json";
  public static final String SPYDRA_CONFIGURATION_FILE_NAME = "spydra_conf.json";
  public static final String DEFAULT_DATAPROC_ARGUMENT_FILE_NAME = "dataproc_defaults.json";

  public static SpydraArgument loadArguments(String fileName)
      throws IOException, URISyntaxException {
    ClassLoader classLoader = SpydraArgumentUtil.class.getClassLoader();
    try (InputStream is = classLoader.getResourceAsStream(fileName)) {
      String json = new String(IOUtils.toByteArray(is));
      return JsonHelper.fromString(json, SpydraArgument.class);
    }
  }

  static boolean configurationExists(String fileName) {
    ClassLoader classLoader = SpydraArgumentUtil.class.getClassLoader();
    return classLoader.getResource(fileName) != null;
  }

  public static SpydraArgument mergeConfigurations(SpydraArgument arguments, String userId)
      throws IOException, URISyntaxException {

    SpydraArgument configuration = loadArguments(BASE_CONFIGURATION_FILE_NAME);
    boolean hasConfigurationInClasspath = configurationExists(SPYDRA_CONFIGURATION_FILE_NAME);
    LOGGER.debug("Spydra configuration found in classpath?: {}", hasConfigurationInClasspath);
    if (hasConfigurationInClasspath) {
      configuration =
          SpydraArgument.merge(configuration, loadArguments(SPYDRA_CONFIGURATION_FILE_NAME));
    }

    SpydraArgument mergedForChecking = SpydraArgument.merge(configuration, arguments);
    if (!mergedForChecking.getCluster().name.isPresent()
        && mergedForChecking.getClusterType() == DATAPROC) {
      SpydraArgument defaultDataprocConfiguration = loadArguments(DEFAULT_DATAPROC_ARGUMENT_FILE_NAME);
      configuration = SpydraArgument.merge(configuration, defaultDataprocConfiguration);

      if (userId != null) {
        configuration.getCluster().getOptions().put(OPTION_SERVICE_ACCOUNT, userId);
      }
    }

    return SpydraArgument.merge(configuration, arguments);
  }

  public static void checkRequiredArguments(SpydraArgument arguments, boolean isOnPremiseInvocation,
      boolean isStaticInvocation) throws IllegalArgumentException {

    boolean isDynamicInvocation = !isOnPremiseInvocation && !isStaticInvocation;
    if (isDynamicInvocation) {
      arguments.clientId.orElseThrow(() ->
          new IllegalArgumentException("client_id needs to be set"));
      arguments.logBucket.orElseThrow(() ->
          new IllegalArgumentException("log_bucket needs to be set"));
      arguments.heartbeatIntervalSeconds.orElseThrow(() ->
          new IllegalArgumentException("heartbeat_interval_seconds needs to be set"));
      arguments.collectorTimeoutMinutes.orElseThrow(() ->
          new IllegalArgumentException("collector_timeout_minutes needs to be set"));
      arguments.historyTimeout.orElseThrow(() ->
          new IllegalArgumentException("history_timeout needs to be set"));
      if (!arguments.cluster.getOptions().containsKey(SpydraArgument.OPTION_PROJECT)) {
        throw new IllegalArgumentException("cluster.options.project needs to be set");
      }

      arguments.region.orElseThrow(() ->
          new IllegalArgumentException("region needs to be set"));
      if (arguments.getRegion().equals("global")) {
        LOGGER.info("Consider omitting defaultZones and cluster.options.zone in your configuration "
                    + "for the auto-zone selector to balance between zones automatically. "
                    + "See https://cloud.google.com/dataproc/docs/concepts/auto-zone");
        if (!arguments.cluster.getOptions().containsKey(SpydraArgument.OPTION_ZONE)
            && arguments.defaultZones.isEmpty()) {
          throw new IllegalArgumentException(
              "Either cluster.options.zone or defaultZones needs to be set");
        }
      }
    }

    if (isStaticInvocation) {
      if (!arguments.submit.getOptions().containsKey(SpydraArgument.OPTION_PROJECT)) {
        throw new IllegalArgumentException("submit.options.project needs to be set");
      }
    }

    if (isOnPremiseInvocation) {
      if (arguments.getSubmit().getOptions().containsKey(SpydraArgument.OPTION_JARS)) {
        throw new IllegalArgumentException(
            "Setting the jars option is not supported when submitting to onpremise");
      }
      if (arguments.getSubmit().getOptions().containsKey(SpydraArgument.OPTION_FILES)) {
        throw new IllegalArgumentException(
            "Setting the files option is not supported when submitting to onpremise");
      }
    }

    if (arguments.getCluster().name.isPresent()) {
      throw new IllegalArgumentException("cluster.name should never be set by the user. Set " +
          "submit.options." + SpydraArgument.OPTION_CLUSTER + " if you want to use a static cluster");
    }

    arguments.metricClass.orElseThrow(() ->
        new IllegalArgumentException("metric_class needs to be set"));
    arguments.clusterType.orElseThrow(() ->
        new IllegalArgumentException("cluster_type needs to be set"));
    arguments.jobType.orElseThrow(() ->
        new IllegalArgumentException("job_type needs to be set"));

    arguments.autoScaler.ifPresent(autoScaler -> {
      autoScaler.interval.orElseThrow(() ->
          new IllegalArgumentException("auto_scaler.interval needs to be set"));
      autoScaler.max.orElseThrow(() ->
          new IllegalArgumentException("auto_scaler.max needs to be set"));
      autoScaler.factor.orElseThrow(() ->
          new IllegalArgumentException("auto_scaler.factor needs to be set"));
      autoScaler.downscale.orElseThrow(()
          -> new IllegalArgumentException("auto_scaler.downscale needs to be set"));
    });
    arguments.pooling.ifPresent(pooling -> {
      pooling.limit.orElseThrow(() ->
          new IllegalArgumentException("pooling.limit needs to be set"));
      pooling.maxAge.orElseThrow(() ->
          new IllegalArgumentException("pooling.max_age needs to be set"));
    });
  }
}
