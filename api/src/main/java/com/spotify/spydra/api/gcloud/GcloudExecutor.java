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

package com.spotify.spydra.api.gcloud;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.spotify.spydra.api.model.Cluster;
import com.spotify.spydra.api.process.ProcessHelper;
import com.spotify.spydra.model.JsonHelper;
import com.spotify.spydra.model.SpydraArgument;
import com.spotify.spydra.util.GcpUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import jdk.nashorn.tools.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcloudExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(GcloudExecutor.class);

  private static final String DEFAULT_GCLOUD_COMMAND = "gcloud";

  private final String baseCommand;

  private boolean dryRun = false;

  public GcloudExecutor() {
    this.baseCommand = DEFAULT_GCLOUD_COMMAND;
  }

  public Optional<Cluster> createCluster(String name, String region, Map<String, String> args)
      throws IOException {
    Map<String, String> createOptions = new HashMap<>(args);
    createOptions.put(SpydraArgument.OPTION_REGION, region);
    List<String> command = Arrays.asList("--format=json", "beta", "dataproc", "clusters", "create", name);
    StringBuilder outputBuilder = new StringBuilder();
    boolean success = ProcessHelper.executeForOutput(
        buildCommand(command, createOptions, Collections.emptyList()),
        outputBuilder);
    String output = outputBuilder.toString();
    if (success) {
      Cluster cluster = JsonHelper.objectMapper()
          .setPropertyNamingStrategy(PropertyNamingStrategy.LOWER_CAMEL_CASE)
          .readValue(output, Cluster.class);
      return Optional.of(cluster);
    } else {
      if(output.contains("ALREADY_EXISTS")){
        throw new GcloudClusterAlreadyExistsException(output);
      }
      LOGGER.error("Dataproc cluster creation call failed. Command line output:");
      LOGGER.error(output);
      return Optional.empty();
    }
  }

  public boolean deleteCluster(String name, String region, Map<String, String> args) throws IOException {
    Map<String, String> deleteOptions = new HashMap<>(args);
    deleteOptions.put(SpydraArgument.OPTION_REGION, region);
    return execute(Arrays.asList("dataproc", "clusters", "delete", name,
        createOption("async", "")),
        deleteOptions, Collections.emptyList());
  }

  public boolean submit(String type, Optional<String> pyFile, String region, Map<String, String> options, List<String> jobArgs)
      throws IOException {
    Map<String, String> submitOptions = new HashMap<>(options);
    submitOptions.put(SpydraArgument.OPTION_REGION, region);
    List<String> submitCommand = new ArrayList<>(Arrays.asList("dataproc", "jobs", "submit", type));
    if (type.equals(SpydraArgument.JOB_TYPE_PYSPARK)) {
      // JOB_TYPE_PYSPARK is special, it has a positional argument :|
      submitCommand.add(pyFile.orElseThrow(() -> new IllegalArgumentException(
          "Somehow pyFile was not set when running a pyspark job. " +
          "This should've been caught in SpydraArgumentUtil#checkRequiredArguments already!"
      )));
    }
    return execute(submitCommand, submitOptions, jobArgs);
  }

  private List<String> buildCommand(List<String> commands, Map<String, String> options, List<String> jobArgs) {
    List<String> command = new ArrayList<>();
    command.add(this.baseCommand);
    final GcpUtils gcpUtils = new GcpUtils();
    gcpUtils.getJsonCredentialsPath().ifPresent(ignored ->
        gcpUtils.getUserId().ifPresent(userId -> {
          command.add("--account");
          command.add(userId);
        })
    );

    command.addAll(commands);
    command.add(createOption("quiet", ""));
    options.forEach((key, value) -> command.add(createOption(key, value)));
    if (jobArgs.size() != 0) {
      command.add("--");
      command.addAll(jobArgs);
    }

    return command;
  }

  private boolean execute(List<String> commands, Map<String, String> options, List<String> jobArgs)
      throws IOException {
    List<String> command = buildCommand(commands, options, jobArgs);
    if (this.dryRun) {
      System.out.println(String.join(" ", command));
      return true;
    } else {
      return ProcessHelper.executeCommand(command) == Shell.SUCCESS;
    }
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

  public List<Cluster> listClusters(String project, String region, Map<String,String> filters) throws IOException {
    List<String> command = new ArrayList<>(Arrays.asList("dataproc", "clusters", "list", "--format=json"));
    Map<String, String> options = new HashMap<>();
    options.put(SpydraArgument.OPTION_PROJECT, project);
    options.put(SpydraArgument.OPTION_REGION, region);

    if(filters != null && !filters.isEmpty()) {
      StringJoiner filterItems = new StringJoiner(" AND ");
      filters.forEach((key, value) -> {
        //Allows for label filters to not specify a value to match "anything" (just check if exists)
        if(value == null || value.isEmpty()) {
          value = "*";
        }
        filterItems.add(String.format("%s = %s", key, value));
      });
      options.put(SpydraArgument.OPTIONS_FILTER, filterItems.toString());
    }

    StringBuilder outputBuilder = new StringBuilder();
    boolean success = ProcessHelper.executeForOutput(
        buildCommand(command, options, Collections.emptyList()),
        outputBuilder
    );
    String output = outputBuilder.toString();
    if (success) {
      Cluster[] clusters = JsonHelper.objectMapper()
          .setPropertyNamingStrategy(PropertyNamingStrategy.LOWER_CAMEL_CASE)
          .readValue(output, Cluster[].class);
      return Arrays.asList(clusters);
    } else {
      LOGGER.error("Dataproc cluster listing call failed. Command line output:");
      LOGGER.error(output);
      throw new IOException("Failed to list clusters. Gcloud call failed.");
    }
  }
}
