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
import com.spotify.spydra.api.model.Job;
import com.spotify.spydra.api.process.ProcessHelper;
import com.spotify.spydra.api.process.ProcessResult;
import com.spotify.spydra.api.process.ProcessService;
import com.spotify.spydra.model.JsonHelper;
import com.spotify.spydra.model.SpydraArgument;
import com.spotify.spydra.util.GcpUtils;
import com.spotify.spydra.util.SpydraArgumentUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import jdk.nashorn.tools.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcloudExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(GcloudExecutor.class);

  private static final String DEFAULT_GCLOUD_COMMAND = "gcloud";

  private final ProcessService processService;
  private final String baseCommand;

  private boolean dryRun = false;

  public GcloudExecutor() {
    this(new ProcessHelper());
  }

  public GcloudExecutor(ProcessService processService) {
    this.baseCommand = DEFAULT_GCLOUD_COMMAND;
    this.processService = processService;
  }

  public Optional<Cluster> createCluster(String name, String region, Map<String, String> args)
      throws IOException {
    Map<String, String> createOptions = new HashMap<>(args);
    createOptions.put(SpydraArgument.OPTION_REGION, region);
    List<String> command = Arrays.asList(
        "--format=json", "beta", "dataproc", "clusters", "create", name);
    ProcessResult result = processService.executeForOutput(
        buildCommand(command, createOptions, Collections.emptyList()));
    String output = result.getOutput();
    if (result.isSuccess()) {
      Cluster cluster = JsonHelper.objectMapper()
          .setPropertyNamingStrategy(PropertyNamingStrategy.LOWER_CAMEL_CASE)
          .readValue(output, Cluster.class);
      return Optional.of(cluster);
    } else {
      if (output.contains("ALREADY_EXISTS")) {
        throw new GcloudClusterAlreadyExistsException(output);
      }
      LOGGER.error("Dataproc cluster creation call failed. Command line output:");
      LOGGER.error(output);
      return Optional.empty();
    }
  }

  public boolean deleteCluster(String name, String region, Map<String, String> args)
      throws IOException {
    Map<String, String> deleteOptions = new HashMap<>(args);
    deleteOptions.put(SpydraArgument.OPTION_REGION, region);
    return execute(
        Arrays.asList("dataproc", "clusters", "delete", name, createOption("async", "")),
        deleteOptions,
        Collections.emptyList()
    );
  }

  public boolean submit(
      String type,
      Optional<String> pyFile,
      String region,
      Map<String, String> options,
      List<String> jobArgs
  ) throws IOException {
    Map<String, String> submitOptions = new HashMap<>(options);
    submitOptions.put(SpydraArgument.OPTION_REGION, region);
    List<String> submitCommand = new ArrayList<>(Arrays.asList("dataproc", "jobs", "submit", type));
    if (type.equals(SpydraArgument.JOB_TYPE_PYSPARK)) {
      // JOB_TYPE_PYSPARK is special, it has a positional argument :|
      submitCommand.add(pyFile.orElseThrow(() -> new IllegalArgumentException(
          "Somehow pyFile was not set when running a pyspark job. "
          + "This should've been caught in SpydraArgumentUtil#checkRequiredArguments already!"
      )));
    }
    return execute(submitCommand, submitOptions, jobArgs);
  }

  private List<String> buildCommand(
      List<String> commands,
      Map<String, String> options,
      List<String> jobArgs
  ) {
    List<String> command = new ArrayList<>();
    command.add(this.baseCommand);
    final GcpUtils gcpUtils = new GcpUtils();
    gcpUtils.getJsonCredentialsPath().ifPresent(
        ignored -> gcpUtils.getUserId().ifPresent(userId -> {
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
      return processService.execute(command) == Shell.SUCCESS;
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

  public List<Job> listJobs(String project, String region, Map<String,String> filters,
        Optional<Integer> limit, Optional<String> sortBy)
      throws IOException {

    final List<String> command = Arrays.asList("dataproc", "jobs", "list", "--format=json");
    Map<String, String> options = new HashMap<>();
    options.put(SpydraArgument.OPTION_PROJECT, project);
    options.put(SpydraArgument.OPTION_REGION, region);
    limit.ifPresent(l -> options.put("limit", String.valueOf(l)));
    sortBy.ifPresent(s -> options.put("sort-by", s));

    if (filters != null && !filters.isEmpty()) {
      String labelItems = SpydraArgumentUtil.joinFilters(filters);
      options.put(SpydraArgument.OPTIONS_FILTER, labelItems);
    }

    ProcessResult result = processService.executeForOutput(
        buildCommand(command, options, Collections.emptyList()));
    String output = result.getOutput();
    if (result.isSuccess()) {
      Job[] jobs = JsonHelper.objectMapper()
              .setPropertyNamingStrategy(PropertyNamingStrategy.LOWER_CAMEL_CASE)
              .readValue(output, Job[].class);
      return Arrays.asList(jobs);
    } else {
      LOGGER.error("Dataproc job listing call failed. Command line output:");
      LOGGER.error(output);
      throw new IOException("Failed to list jobs. Gcloud call failed.");
    }

  }

  public List<Cluster> listClusters(String project, String region, Map<String, String> filters)
      throws IOException {
    final List<String> command = Arrays.asList("dataproc", "clusters", "list", "--format=json");
    Map<String, String> options = new HashMap<>();
    options.put(SpydraArgument.OPTION_PROJECT, project);
    options.put(SpydraArgument.OPTION_REGION, region);

    if (filters != null && !filters.isEmpty()) {
      String filterItems = SpydraArgumentUtil.joinFilters(filters);
      options.put(SpydraArgument.OPTIONS_FILTER, filterItems);
    }

    ProcessResult result = processService.executeForOutput(
        buildCommand(command, options, Collections.emptyList()));
    String output = result.getOutput();
    if (result.isSuccess()) {
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

  public boolean waitForOutput(String region, String jobId) throws IOException {
    final List<String> command = Arrays.asList("dataproc", "jobs", "wait", jobId);
    Map<String, String> options = new HashMap<>();
    options.put(SpydraArgument.OPTION_REGION, region);

    int exitCode = processService.execute(buildCommand(command, options, Collections.emptyList()));
    if (exitCode != 0) {
      LOGGER.error("Dataproc wait for job failed.");
      return false;
    } else {
      return true;
    }
  }
}
