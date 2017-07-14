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

package com.spotify.spydra.api.gcloud;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.spotify.spydra.api.model.Cluster;
import com.spotify.spydra.api.process.ProcessHelper;
import com.spotify.spydra.model.JsonHelper;
import com.spotify.spydra.model.SpydraArgument;

import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jdk.nashorn.tools.Shell;

public class GcloudExecutor {

  private static final String DEFAULT_GCLOUD_COMMAND = "gcloud";

  private final String baseCommand;

  private boolean dryRun = false;

  public GcloudExecutor() {
    this(DEFAULT_GCLOUD_COMMAND);
  }

  public GcloudExecutor(String gcloudCommand) {
    baseCommand = gcloudCommand;
  }

  public Optional<Cluster> createCluster(String name, String region, Map<String, String> args) throws IOException {
    Map<String, String> createOptions = new HashMap<>(args);
    createOptions.put(SpydraArgument.OPTION_REGION, region);
    ImmutableList<String> command = ImmutableList.of("--format=json", "dataproc", "clusters", "create", name);

    String jsonString= ProcessHelper.executeForOutput(buildCommand(command, createOptions, Collections.EMPTY_LIST));
    Cluster cluster = JsonHelper.objectMapper()
        .setPropertyNamingStrategy(PropertyNamingStrategy.LOWER_CAMEL_CASE)
        .readValue(jsonString, Cluster.class);
    return Optional.of(cluster);
  }

  public boolean deleteCluster(String name, String region, Map<String, String> args) throws IOException {
    Map<String, String> deleteOptions = new HashMap<>(args);
    deleteOptions.put(SpydraArgument.OPTION_REGION, region);
    return execute(ImmutableList.of("dataproc", "clusters", "delete", name,
        createOption("async", "")),
        deleteOptions, Collections.emptyList());
  }

  public boolean submit(String type, String region, Map<String, String> options, List<String> jobArgs)
      throws IOException {
    Map<String, String> submitOptions = new HashMap<>(options);
    submitOptions.put(SpydraArgument.OPTION_REGION, region);
    return execute(ImmutableList.of("dataproc", "jobs", "submit", type), submitOptions, jobArgs);
  }

  private ArrayList<String> buildCommand(List<String> commands, Map<String, String> options, List<String> jobArgs) {

    ArrayList<String> command = Lists.newArrayList(this.baseCommand);

    command.addAll(commands);
    command.add(createOption("quiet", ""));
    options.entrySet().forEach(entry -> command.add(createOption(entry.getKey(), entry.getValue())));
    if (jobArgs.size() != 0) {
      command.add("--");
      jobArgs.forEach(command::add);
    }

    return command;
  }

  private boolean execute(List<String> commands, Map<String, String> options, List<String> jobArgs)
      throws IOException {
    ArrayList<String> command = buildCommand(commands, options, jobArgs);

    if (this.dryRun) {
      System.out.println(StringUtils.join(command, StringUtils.SPACE));
      return true;
    } else {
      return ProcessHelper.executeCommand(command) == Shell.SUCCESS;
    }
  }

  public String getMasterNode(String project, String region, String clusterName)
      throws IOException {
    ArrayList<String> command = Lists.newArrayList(
        "--format=json", "dataproc", "clusters", "describe", clusterName);

    Map<String, String> options = ImmutableMap.of(
        SpydraArgument.OPTION_PROJECT, project,
        SpydraArgument.OPTION_REGION, region);

    String jsonString = ProcessHelper.executeForOutput(buildCommand(command, options, Collections.EMPTY_LIST));
    Cluster cluster = JsonHelper.objectMapper()
        .setPropertyNamingStrategy(PropertyNamingStrategy.LOWER_CAMEL_CASE)
        .readValue(jsonString, Cluster.class);

    // If we have a cluster we have a master
    return cluster.config.masterConfig.instanceNames.get(0);
  }

  public boolean updateMetadata(
      String node, Map<String, String> options, String key, String value) throws IOException {
    List<String> command = Lists.newArrayList(
        "compute", "instances", "add-metadata", node);
    Map<String, String> metadataOptions = new HashMap<>(options);

    metadataOptions.put(SpydraArgument.OPTION_METADATA, key + "=" + value);

    return execute(command, metadataOptions, Collections.EMPTY_LIST);
  }

  private static String createOption(String optionName, String optionValue) {
    if (optionValue.length() > 0) {
      return "--" + optionName + "=" + optionValue;
    } else {
      return "--" + optionName;
    }
  }

  public void dryRun(boolean dryRun) {
    this.dryRun = dryRun;
  }

  public Collection<Cluster> listClusters(String project, String region) throws IOException {
    List<String> command = Lists.newArrayList(
        "dataproc", "clusters", "list", "--format=json");
    Map<String, String> options = ImmutableMap.of(
        SpydraArgument.OPTION_PROJECT, project,
        SpydraArgument.OPTION_REGION, region);

    String jsonString = ProcessHelper.executeForOutput(buildCommand(command, options, Collections.EMPTY_LIST));
    Cluster[] clusters = JsonHelper.objectMapper()
        .setPropertyNamingStrategy(PropertyNamingStrategy.LOWER_CAMEL_CASE)
        .readValue(jsonString, Cluster[].class);
    return Arrays.asList(clusters);
  }
}
