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

package com.spotify.spydra.model;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

public class SpydraArgument {
  public static final String OPTION_JAR = "jar";
  public static final String OPTION_JARS = "jars";
  public static final String OPTION_CLASS = "class";
  public static final String OPTION_PROPERTIES = "properties";
  public static final String OPTION_FILES = "files";
  public static final String OPTION_PROJECT = "project";
  public static final String OPTION_REGION = "region";
  public static final String OPTION_ZONE = "zone";
  public static final String OPTION_METADATA = "metadata";
  public static final String OPTION_CLUSTER = "cluster";
  public static final String OPTION_INIT_ACTIONS = "initialization-actions";
  public static final String OPTION_SCOPES = "scopes";
  public static final String OPTION_NETWORK = "network";
  public static final String OPTION_SUBNET = "subnet";
  public static final String OPTION_WORKER_MACHINE_TYPE = "worker-machine-type";
  public static final String OPTION_MASTER_MACHINE_TYPE = "master-machine-type";
  public static final String OPTION_NUM_WORKERS = "num-workers";
  public static final String OPTION_CLUSTER_LABELS = "labels";
  public static final String OPTION_JOB_LABELS = "labels";
  public static final String OPTION_TAGS = "tags";
  public static final String OPTION_ACCOUNT = "account";
  public static final String OPTION_SERVICE_ACCOUNT = "service-account";
  public static final String OPTION_JOB_ID = "id";
  public static final String OPTION_MAX_IDLE = "max-idle";
  public static final String OPTIONS_FILTER = "filter";
  public static final String OPTIONS_FILTER_LABEL_PREFIX = "labels.";
  public static final String OPTIONS_DEDUPLICATING_LABEL = "spydra-dedup-id";

  public static final String JOB_TYPE_HADOOP = "hadoop";
  public static final String JOB_TYPE_PYSPARK = "pyspark";

  public static final String CLIENT_ID_PROPERTIES_PLACEHOLDER = "${CLIENT_ID}";
  public static final String LOG_BUCKET_PROPERTIES_PLACEHOLDER = "${LOG_BUCKET}";
  public static final String UUID_PLACEHOLDER = "${UUID}";

  public static final String OPTION_DRYRUN = "dry-run";


  // Required arguments
  public Optional<String> clientId = Optional.empty();
  public Optional<String> logBucket = Optional.empty();
  public Optional<String> metricClass = Optional.empty();
  public Optional<String> region = Optional.empty();

  // Optional Spydra arguments
  public Optional<ClusterType> clusterType = Optional.empty();
  public Optional<Integer> historyTimeout = Optional.empty();
  public Optional<Boolean> dryRun = Optional.of(false);
  public Optional<AutoScaler> autoScaler = Optional.empty();
  public Optional<Pooling> pooling = Optional.empty();
  public Optional<Long> deduplicationMaxAge = Optional.empty();

  // Dataproc arguments
  public Cluster cluster = new Cluster();
  public Submit submit = new Submit();

  // Optional Dataproc arguments
  public Optional<String> jobType = Optional.empty();

  public class Cluster {
    public Optional<String> name = Optional.empty();
    public Map<String, String> options = new HashMap<>();

    public String getName() {
      return name.get();
    }

    public Map<String, String> getOptions() {
      return options;
    }

    public void setName(String name) {
      this.name = Optional.of(name);
    }

    public void setOptions(Map<String, String> options) {
      this.options.putAll(options);
    }

    // Below are convenience functions when using this from the Java API.

    public void network(String network) {
      this.options.put(OPTION_NETWORK, network);
    }

    public void subnet(String subnet) {
      this.options.put(OPTION_SUBNET, subnet);
    }

    public void zone(String zone) {
      this.options.put(OPTION_ZONE, zone);
    }

    public void workerMachineType(String workerMachineType) {
      this.options.put(OPTION_WORKER_MACHINE_TYPE, workerMachineType);
    }

    public void masterMachineType(String workerMachineType) {
      this.options.put(OPTION_MASTER_MACHINE_TYPE, workerMachineType);
    }

    public void numWorkers(Integer numWorkers) {
      this.options.put(OPTION_NUM_WORKERS, numWorkers.toString());
    }

    public void project(String project) {
      this.options.put(OPTION_PROJECT, project);
    }
  }

  public class Submit {
    public Map<String, String> options = new HashMap<>();
    public Optional<List<String>> jobArgs = Optional.empty();
    public Optional<String> pyFile = Optional.empty();

    public Map<String, String> getOptions() {
      return options;
    }

    public List<String> getJobArgs() {
      return jobArgs.orElse(Collections.emptyList());
    }

    public void setOptions(Map<String, String> options) {
      this.options.putAll(options);
    }

    public void setJobArgs(List<String> jobArgs) {
      this.jobArgs = Optional.of(jobArgs);
    }

    public Map<String, String> getLabels() {
      return parseLabels(options);
    }

    // Below are convenience functions when using this from the Java API.

    public void jar(String mainJar) {
      this.options.put(OPTION_JAR, mainJar);
    }

    void addJarToJars(String jar) {
      this.options.merge(OPTION_JARS, jar, (oldJars, newJar) -> oldJars + "," + newJar);
    }

    public void jars(Iterable<String> jars) {
      jars.forEach(this::addJarToJars);
    }

    public void mainClass(String mainClass) {
      options.put(OPTION_CLASS, mainClass);
    }

    public void addFile(String file) {
      options.merge(OPTION_FILES, file, (oldFiles, newFile) -> oldFiles + "," + newFile);
    }

    public void addProperty(String propertyKey, String propertyValue) {
      String property = propertyKey + "=" + propertyValue;
      options.merge(OPTION_PROPERTIES, property,
          (oldProperties, newProperty) -> oldProperties + "," + newProperty);
    }

    public void setLabels(String labelsOption) {
      options.put(OPTION_JOB_LABELS, labelsOption);
    }

  }

  public static class AutoScaler {
    public Optional<Integer> interval = Optional.empty();
    public Optional<Integer> max = Optional.empty();
    public Optional<Double> factor = Optional.empty();
    public Optional<Boolean> downscale = Optional.empty();
    public Optional<Integer> downscaleTimeout = Optional.empty();

    public Integer getInterval() {
      return interval.get();
    }

    public void setInterval(Integer interval) {
      this.interval = Optional.of(interval);
    }

    public Integer getMax() {
      return max.get();
    }

    public void setMax(Integer max) {
      this.max = Optional.of(max);
    }

    public Double getFactor() {
      return factor.get();
    }

    public void setFactor(Double factor) {
      this.factor = Optional.of(factor);
    }

    public Boolean getDownscale() {
      return downscale.get();
    }

    public void setDownscale(Boolean downscale) {
      this.downscale = Optional.of(downscale);
    }

    public Integer getDownscaleTimeout() {
      return downscaleTimeout.get();
    }

    public void setDownscaleTimeout(Integer downscaleTimeout) {
      this.downscaleTimeout = Optional.of(downscaleTimeout);
    }
  }

  public static class Pooling {
    public Optional<Integer> limit;
    public Optional<Duration> maxAge;

    public Integer getLimit() {
      return limit.get();
    }

    public void setLimit(Integer limit) {
      this.limit = Optional.of(limit);
    }

    public Duration getMaxAge() {
      return maxAge.get();
    }

    public void setMaxAge(Duration maxAge) {
      this.maxAge = Optional.of(maxAge);
    }

  }

  /**
   * Merges two set of arguments, with the values in the first argument overwritten by the second.
   *
   * @param first  A set of Spydra arguments. These are the defaults that will be overwritten by the
   *               values in second if the key exists in both.
   * @param second A set of Spydra arguments. The second argument overwrites the first.
   * @return A set (instance) of merged Spydra arguments.
   */
  public static SpydraArgument merge(SpydraArgument first, SpydraArgument second) {
    if (first == null) {
      return second;
    } else if (second == null) {
      return first;
    }

    SpydraArgument merged = new SpydraArgument();

    if (second.clientId.isPresent()) {
      merged.clientId = second.clientId;
    } else {
      merged.clientId = first.clientId;
    }

    if (second.logBucket.isPresent()) {
      merged.logBucket = second.logBucket;
    } else {
      merged.logBucket = first.logBucket;
    }

    if (second.metricClass.isPresent()) {
      merged.metricClass = second.metricClass;
    } else {
      merged.metricClass = first.metricClass;
    }

    if (second.clusterType.isPresent()) {
      merged.clusterType = second.clusterType;
    } else {
      merged.clusterType = first.clusterType;
    }

    if (second.historyTimeout.isPresent()) {
      merged.historyTimeout = second.historyTimeout;
    } else {
      merged.historyTimeout = first.historyTimeout;
    }

    if (second.autoScaler.isPresent()) {
      merged.autoScaler = second.autoScaler;
    } else if (first.autoScaler.isPresent()) {
      merged.autoScaler = first.autoScaler;
    }

    if (second.dryRun.isPresent()) {
      merged.dryRun = second.dryRun;
    } else {
      merged.dryRun = first.dryRun;
    }

    if (second.deduplicationMaxAge.isPresent()) {
      merged.deduplicationMaxAge = second.deduplicationMaxAge;
    } else {
      merged.deduplicationMaxAge = first.deduplicationMaxAge;
    }

    if (second.jobType.isPresent()) {
      merged.jobType = second.jobType;
    } else {
      merged.jobType = first.jobType;
    }

    if (second.cluster.name.isPresent()) {
      merged.cluster.name = second.cluster.name;
    } else {
      merged.cluster.name = first.cluster.name;
    }

    merged.cluster.options.putAll(first.cluster.options);
    mergeOptions(merged.cluster.options, second.cluster.options);

    merged.submit.options.putAll(first.submit.options);
    mergeOptions(merged.submit.options, second.submit.options);

    if (second.submit.jobArgs.isPresent()) {
      merged.submit.jobArgs = second.submit.jobArgs;
    } else {
      merged.submit.jobArgs = first.submit.jobArgs;
    }

    if (second.submit.pyFile.isPresent()) {
      merged.submit.pyFile = second.submit.pyFile;
    } else {
      merged.submit.pyFile = first.submit.pyFile;
    }

    if (second.pooling.isPresent()) {
      merged.pooling = second.pooling;
    } else {
      merged.pooling = first.pooling;
    }

    if (second.region.isPresent()) {
      merged.region = second.region;
    } else {
      merged.region = first.region;
    }

    return merged;
  }

  public void replacePlaceholders() {
    replacePlaceholders(getCluster().getOptions());
    replacePlaceholders(getSubmit().getOptions());
  }

  private void replacePlaceholders(Map<String, String> options) {
    if (options.containsKey(OPTION_PROPERTIES)) {
      String properties = options.get(OPTION_PROPERTIES);
      if (clientId.isPresent()) {
        properties = properties.replace(CLIENT_ID_PROPERTIES_PLACEHOLDER, getClientId());
      }
      if (logBucket.isPresent()) {
        properties = properties.replace(LOG_BUCKET_PROPERTIES_PLACEHOLDER, getLogBucket());
        properties = properties.replace(UUID_PLACEHOLDER, UUID.randomUUID().toString());
      }
      options.put(OPTION_PROPERTIES, properties);
    }
  }

  /**
   * Merges given option maps into one. The keys in the map given as first parameter will be
   * overwritten by the keys in the second, if the keys exist in both maps.
   *
   * @param mergeIntoThis Options map into which the second set of options will be merged.
   * @param options       Options map to be merged with the options in the first argument.
   */
  private static void mergeOptions(Map<String, String> mergeIntoThis, Map<String, String> options) {
    for (Map.Entry<String, String> option : options.entrySet()) {
      String key = option.getKey();
      String value = option.getValue();

      addOption(mergeIntoThis, key, value);
    }
  }

  public static void addOption(Map<String, String> options, String key, String value) {
    if (isListSupported(key)) {
      options.put(key, joinValues(options.get(key), value));
    } else {
      options.put(key, value);
    }
  }

  private static String joinValues(String v1, String v2) {
    // At most one value can be null since it is grabbed from a Map where it may not exist.
    if (v1 == null || v1.length() == 0) {
      return v2;
    }
    if (v2 == null || v2.length() == 0) {
      return v1;
    }
    return v1 + "," + v2;
  }

  public Properties clusterProperties() {
    return parseProperties(getCluster().getOptions());
  }

  public Properties submitProperties() {
    return parseProperties(getCluster().getOptions());
  }

  private Properties parseProperties(Map<String, String> options) {
    Properties properties = new Properties();
    if (options.containsKey(OPTION_PROPERTIES)) {
      for (String property : options.get(OPTION_PROPERTIES).split(",")) {
        String[] split = property.split("=");
        properties.put(split[0], split[1]);
      }
    }

    return properties;
  }

  private static Map<String,String> parseLabels(Map<String,String> options) {
    Map<String, String> labels = new HashMap<>();
    if (options.containsKey(OPTION_JOB_LABELS)) {
      for (String labelValue : options.get(OPTION_JOB_LABELS).split(",")) {
        String[] split = labelValue.split("=");
        labels.put(split[0].trim(), split[1].trim());
      }
    }
    return labels;
  }

  // Special treatment for options that allow a list of values
  private static boolean isListSupported(String key) {
    switch (key) {
      case OPTION_INIT_ACTIONS:
      case OPTION_SCOPES:
      case OPTION_METADATA:
      case OPTION_CLUSTER_LABELS:
      case OPTION_TAGS:
      case OPTION_PROPERTIES:
      case OPTION_JARS:
      case OPTION_FILES:
        return true;
      default:
        return false;
    }
  }

  public String getClientId() {
    return clientId.get();
  }

  public String getLogBucket() {
    return logBucket.get();
  }

  public String getMetricClass() {
    return metricClass.get();
  }

  public String getRegion() {
    return region.get();
  }

  public ClusterType getClusterType() {
    return clusterType.get();
  }

  public Integer getHistoryTimeout() {
    return historyTimeout.get();
  }

  public Boolean isDryRun() {
    return dryRun.get();
  }

  public String getJobType() {
    return jobType.get();
  }

  public Cluster getCluster() {
    return cluster;
  }

  public Submit getSubmit() {
    return submit;
  }


  public AutoScaler getAutoScaler() {
    return autoScaler.get();
  }

  public Pooling getPooling() {
    return pooling.get();
  }

  public void setPooling(Integer limit, Duration maxAge) {
    Pooling pooling = new Pooling();
    pooling.setLimit(limit);
    pooling.setMaxAge(maxAge);
    this.pooling = Optional.of(pooling);
  }

  public void setPooling(Pooling pooling) {
    this.pooling = Optional.of(pooling);
  }

  public boolean isPoolingEnabled() {
    return pooling.isPresent();
  }

  public void setClientId(String clientId) {
    this.clientId = Optional.of(clientId);
  }

  public void setLogBucket(String logBucket) {
    this.logBucket = Optional.of(logBucket);
  }

  public void setMetricClass(String metricClass) {
    this.metricClass = Optional.of(metricClass);
  }

  public void setRegion(String region) {
    this.region = Optional.of(region);
  }

  public void setClusterType(ClusterType clusterType) {
    this.clusterType = Optional.of(clusterType);
  }

  public void setHistoryTimeout(Integer historyTimeout) {
    this.historyTimeout = Optional.of(historyTimeout);
  }

  public void setDryRun(Boolean dryRun) {
    this.dryRun = Optional.of(dryRun);
  }

  public void setDeduplicationMaxAge(long maxAge) {
    this.deduplicationMaxAge = Optional.of(maxAge);
  }

  public Optional<Duration> deduplicationMaxAge() {
    return deduplicationMaxAge.map(ms -> Duration.of(ms, ChronoUnit.SECONDS));
  }

  public void setCluster(Cluster cluster) {
    this.cluster = cluster;
  }

  public void setSubmit(Submit submit) {
    this.submit = submit;
  }

  public void setJobType(String jobType) {
    this.jobType = Optional.of(jobType);
  }

  public void setAutoScaler(AutoScaler autoScaler) {
    this.autoScaler = Optional.of(autoScaler);
  }

  public void setAutoScaler(Double factor, Integer interval, Integer max) {
    setAutoScaler(factor, interval, max, Optional.empty());
  }

  public void setAutoScaler(Double factor, Integer interval, Integer max,
                            Optional<Integer> downscaleTimeout) {
    AutoScaler autoScaler = new AutoScaler();
    autoScaler.setFactor(factor);
    autoScaler.setInterval(interval);
    autoScaler.setMax(max);
    autoScaler.setDownscale(downscaleTimeout.isPresent());
    downscaleTimeout.ifPresent(autoScaler::setDownscaleTimeout);
    setAutoScaler(autoScaler);
  }

}
