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

package com.spotify.spydra.util;

import static com.spotify.spydra.model.ClusterType.DATAPROC;
import static com.spotify.spydra.model.SpydraArgument.OPTION_ACCOUNT;
import static com.spotify.spydra.model.SpydraArgument.OPTION_CLUSTER;
import static com.spotify.spydra.model.SpydraArgument.OPTION_MAX_IDLE;
import static com.spotify.spydra.model.SpydraArgument.OPTION_SERVICE_ACCOUNT;

import com.spotify.spydra.model.ClusterType;
import com.spotify.spydra.model.JsonHelper;
import com.spotify.spydra.model.SpydraArgument;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpydraArgumentUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(SpydraArgumentUtil.class);

  public static final String BASE_CONFIGURATION_FILE_NAME = "defaults.json";
  public static final String SPYDRA_CONFIGURATION_FILE_NAME = "spydra_conf.json";
  public static final String DEFAULT_DATAPROC_ARGUMENT_FILE_NAME = "dataproc_defaults.json";

  public static SpydraArgument loadArguments(String fileName)
      throws IOException, URISyntaxException {
    ClassLoader classLoader = SpydraArgumentUtil.class.getClassLoader();
    try (InputStream is = classLoader.getResourceAsStream(fileName)) {
      if (is == null) {
        throw new IOException("Failed to load arguments from " + fileName);
      }
      String json = new String(IOUtils.toByteArray(is));
      return JsonHelper.fromString(json, SpydraArgument.class);
    }
  }

  static boolean configurationExists(String fileName) {
    ClassLoader classLoader = SpydraArgumentUtil.class.getClassLoader();
    return classLoader.getResource(fileName) != null;
  }

  private static SpydraArgument mergeConfigsFromPath(String[] configFilesInClassPath,
                                                     SpydraArgument arguments)
      throws IOException, URISyntaxException {
    SpydraArgument config = null;
    for (String configFilePath : configFilesInClassPath) {
      if (configurationExists(configFilePath)) {
        LOGGER.debug("Merge conf found from classpath: {}", configFilePath);
        config = SpydraArgument.merge(config, loadArguments(configFilePath));
      }
    }
    config = SpydraArgument.merge(config, arguments);
    return config;
  }

  public static SpydraArgument mergeConfigurations(
      SpydraArgument arguments, Optional<String> userId
  ) throws IOException, URISyntaxException {
    SpydraArgument baseArgsWithGivenArgs = mergeConfigsFromPath(
            new String[]{BASE_CONFIGURATION_FILE_NAME, SPYDRA_CONFIGURATION_FILE_NAME},
            arguments);
    boolean isDynamicDataprocCluster = !baseArgsWithGivenArgs.getCluster().name.isPresent()
                                       && baseArgsWithGivenArgs.getClusterType() == DATAPROC;
    SpydraArgument outputConfig;
    if (isDynamicDataprocCluster) {
      // Need to merge configs again, as values from SPYDRA_CONFIGURATION_FILE_NAME should
      // overwrite values from DEFAULT_DATAPROC_ARGUMENT_FILE_NAME.
      outputConfig = mergeConfigsFromPath(
          new String[]{BASE_CONFIGURATION_FILE_NAME, DEFAULT_DATAPROC_ARGUMENT_FILE_NAME,
                       SPYDRA_CONFIGURATION_FILE_NAME},
          arguments);
      if (userId.isPresent()) {
        LOGGER.debug(
            "Set user account and service-account for gcloud invocations: {}", userId.get());
        outputConfig.getCluster().getOptions().put(OPTION_ACCOUNT, userId.get());
        outputConfig.getCluster().getOptions().put(OPTION_SERVICE_ACCOUNT, userId.get());
      } else {
        LOGGER.debug("Using application default credentials for gcloud invocations");
      }
    } else {
      outputConfig = baseArgsWithGivenArgs;
    }
    return outputConfig;
  }

  public static SpydraArgument dataprocConfiguration(String clientId, String logBucket,
                                                            String region)
      throws IOException, URISyntaxException {
    SpydraArgument base = new SpydraArgument();
    base.setClusterType(ClusterType.DATAPROC);
    base.setClientId(clientId);
    base.setLogBucket(logBucket);
    base.setRegion(region);
    SpydraArgument defaults = mergeConfigsFromPath(
        new String[]{BASE_CONFIGURATION_FILE_NAME, DEFAULT_DATAPROC_ARGUMENT_FILE_NAME,
                     SPYDRA_CONFIGURATION_FILE_NAME},
        base);

    GcpUtils gcpUtils = new GcpUtils();
    gcpUtils.getUserId().ifPresent(userId -> {
      final Map<String, String> options = defaults.getCluster().getOptions();

      options.put(SpydraArgument.OPTION_ACCOUNT, userId);
      // If we have json credentials on path, add the user as the service account user too
      gcpUtils.getJsonCredentialsPath().ifPresent(
          ignored -> options.put(SpydraArgument.OPTION_SERVICE_ACCOUNT, userId));
    });

    gcpUtils.configureClusterProjectFromCredential(defaults);
    defaults.replacePlaceholders();
    return defaults;
  }

  public static void setDefaultClientIdIfRequired(SpydraArgument arguments)
      throws UnknownHostException {
    if (!arguments.clientId.isPresent()) {
      arguments.setClientId(InetAddress.getLocalHost().getHostName());
    }
  }

  public static void setProjectFromCredentialsIfNotSet(SpydraArgument arguments) {
    arguments.getCluster().getOptions().computeIfAbsent(
        SpydraArgument.OPTION_PROJECT,
        key -> new GcpUtils().getProjectId());
  }

  public static void checkRequiredArguments(SpydraArgument arguments, boolean isOnPremiseInvocation,
      boolean isStaticInvocation) throws IllegalArgumentException {

    boolean isDynamicInvocation = !isOnPremiseInvocation && !isStaticInvocation;
    if (isDynamicInvocation) {
      arguments.clientId.orElseThrow(() ->
          new IllegalArgumentException("client_id needs to be set"));
      arguments.logBucket.orElseThrow(() ->
          new IllegalArgumentException("log_bucket needs to be set"));
      arguments.historyTimeout.orElseThrow(() ->
          new IllegalArgumentException("history_timeout needs to be set"));
      if (!arguments.cluster.getOptions().containsKey(SpydraArgument.OPTION_PROJECT)) {
        throw new IllegalArgumentException("cluster.options.project needs to be set");
      }
      if (!arguments.getCluster().getOptions().containsKey(OPTION_MAX_IDLE)) {
        throw new IllegalArgumentException("cluster.options.max-idle needs to be set");
      }

      arguments.region.orElseThrow(() ->
          new IllegalArgumentException("region needs to be set"));
      if (arguments.getRegion().equals("global")) {
        if (!arguments.cluster.getOptions().containsKey(SpydraArgument.OPTION_ZONE)) {
          throw new IllegalArgumentException(
              "Please define region other than global, or optionally, "
              + "cluster.options.zone in configuration.");
        }
        LOGGER.info("Consider specifying region and omitting cluster.options.zone in your "
            + "configuration for the auto-zone selector to balance between zones automatically. "
            + "See https://cloud.google.com/dataproc/docs/concepts/auto-zone");
      }
      if (SpydraArgument.JOB_TYPE_PYSPARK.equals(arguments.getJobType())) {
        arguments.submit.pyFile.orElseThrow(
            () -> new IllegalArgumentException(
                "pyspark jobs require the submit.py file to be set"));
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
      throw new IllegalArgumentException(
          "cluster.name should never be set by the user. Set "
          + "submit.options." + SpydraArgument.OPTION_CLUSTER
          + " if you want to use a static cluster");
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
      autoScaler.downscale.ifPresent(downscale -> {
        if (downscale) {
          autoScaler.downscaleTimeout.orElseThrow(()
              -> new IllegalArgumentException("auto_scaler.downscale_timeout needs to be set"));
        }
      });
    });
    arguments.pooling.ifPresent(pooling -> {
      pooling.limit.orElseThrow(() ->
          new IllegalArgumentException("pooling.limit needs to be set"));
      pooling.maxAge.orElseThrow(() ->
          new IllegalArgumentException("pooling.max_age needs to be set"));
    });
  }

  public static boolean isOnPremiseInvocation(SpydraArgument arguments) {
    if (!arguments.clusterType.isPresent()) {
      return true;
    }
    return arguments.getClusterType() != DATAPROC;
  }

  public static boolean isStaticInvocation(SpydraArgument arguments) {
    return arguments.getSubmit().getOptions().containsKey(OPTION_CLUSTER);
  }

  public static String joinFilters(Map<String,String> filters) {
    StringJoiner filterItems = new StringJoiner(" AND ");
    filters.forEach((key, value) -> {
      //Allows for label filters to not specify a value to match "anything" (just check if exists)
      if (value == null || value.isEmpty()) {
        value = "*";
      }
      filterItems.add(String.format("%s=%s", key, value));
    });
    return filterItems.toString();
  }

}
