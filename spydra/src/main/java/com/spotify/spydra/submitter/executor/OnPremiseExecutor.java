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

package com.spotify.spydra.submitter.executor;

import static com.spotify.spydra.model.SpydraArgument.JOB_TYPE_HADOOP;
import static com.spotify.spydra.model.SpydraArgument.OPTION_CLASS;
import static com.spotify.spydra.model.SpydraArgument.OPTION_JAR;
import static com.spotify.spydra.model.SpydraArgument.OPTION_PROPERTIES;

import com.google.common.annotations.VisibleForTesting;
import com.spotify.spydra.api.process.ProcessHelper;
import com.spotify.spydra.metrics.Metrics;
import com.spotify.spydra.metrics.MetricsFactory;
import com.spotify.spydra.model.SpydraArgument;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import jdk.nashorn.tools.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OnPremiseExecutor implements Executor {
  private final Metrics metrics;

  static final String BASE_COMMAND = "hadoop jar";
  static final String PROPERTY = "-D";

  private static final Logger LOGGER = LoggerFactory.getLogger(OnPremiseExecutor.class);

  private List<String> command = new ArrayList<>();

  public OnPremiseExecutor() {
    metrics = MetricsFactory.getInstance();
  }

  @VisibleForTesting
  OnPremiseExecutor(Metrics metrics) {
    this.metrics = metrics;
  }

  @VisibleForTesting
  List<String> getCommand(SpydraArgument arguments) {
    addBaseCommand();

    if (arguments.getSubmit().getOptions().containsKey(OPTION_JAR)) {
      addArgument(arguments.getSubmit().getOptions().get(OPTION_JAR));
    }

    if (arguments.getSubmit().getOptions().containsKey(OPTION_CLASS)) {
      addArgument(arguments.getSubmit().getOptions().get(OPTION_CLASS));
    }

    arguments.getSubmit().jobArgs.ifPresent(this::addArguments);

    if (arguments.getSubmit().getOptions().containsKey(OPTION_PROPERTIES)) {
      addProperties(arguments.getSubmit().getOptions().get(OPTION_PROPERTIES));
    }
    return this.command;
  }

  private void addBaseCommand() {
    String[] commands = BASE_COMMAND.split(" ");
    this.command.addAll(Arrays.asList(commands));
  }

  private void addArgument(String value) {
    this.command.add(value);
  }

  private void addProperties(String properties) {
    for (String property : properties.split(",")) {
      this.command.add(PROPERTY);
      this.command.add(property.replaceAll(".*:", ""));
    }
  }

  private void addArguments(List<String> args) {
    this.command.addAll(args);
  }

  @Override
  public boolean submit(SpydraArgument arguments) throws IOException {
    if (!arguments.getJobType().toLowerCase().equals(JOB_TYPE_HADOOP)) {
      throw new IllegalArgumentException("Default executor does only supports Hadoop jobs");
    }

    List<String> command = getCommand(arguments);
    String fullCommand = String.join(" ", command);
    LOGGER.info("Executing command {}", fullCommand);
    boolean result = false;
    try {
      if (arguments.isDryRun()) {
        System.out.println(fullCommand);
        result = true;
      } else {
        result = ProcessHelper.executeCommand(command) == Shell.SUCCESS;
      }
    } finally {
      metrics.jobSubmission(arguments, "on-premise", result);
    }
    return result;
  }
}
