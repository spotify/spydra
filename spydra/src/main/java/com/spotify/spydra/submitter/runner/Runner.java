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

package com.spotify.spydra.submitter.runner;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import com.spotify.spydra.historytools.DumpHistoryCliParser;
import com.spotify.spydra.historytools.DumpLogsCliParser;
import com.spotify.spydra.historytools.HistoryLogUtils;
import com.spotify.spydra.historytools.RunJHSCliParser;
import com.spotify.spydra.historytools.commands.DumpHistoryCommand;
import com.spotify.spydra.historytools.commands.DumpLogsCommand;
import com.spotify.spydra.historytools.commands.RunJHSCommand;
import com.spotify.spydra.metrics.Metrics;
import com.spotify.spydra.metrics.MetricsFactory;
import com.spotify.spydra.model.SpydraArgument;
import com.spotify.spydra.submitter.api.Submitter;
import com.spotify.spydra.util.GcpUtils;
import com.spotify.spydra.util.SpydraArgumentUtil;

import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;

public class Runner {
  private static final Logger LOGGER = LoggerFactory.getLogger(Runner.class);

  private static final GcpUtils gcpUtils = new GcpUtils();

  public static void main(String[] args) throws Exception {

    if (args.length == 0) {
      LOGGER.error("No command supplied.");
      System.exit(1);
    }

    String command = args[0];
    args = Arrays.copyOfRange(args, 1, args.length);

    switch (command) {
      case CliConsts.SUBMIT_CMD_NAME:
        runSubmit(args);
        break;
      case CliConsts.LOGS_CMD_NAME:
        runDumpLogs(args);
        break;
      case CliConsts.DUMP_HISTORY_CMD_NAME:
        runDumpHistory(args);
        break;
      case CliConsts.RUN_JHS_CMD_NAME:
        runHistoryServer(args);
        break;
      default:
        LOGGER.error("Unknown command: " + command);
        System.out.println("Unsupported command. Possible commands are: " + Joiner.on(",")
            .join(Lists.newArrayList(CliConsts.SUBMIT_CMD_NAME, CliConsts.LOGS_CMD_NAME,
                CliConsts.DUMP_HISTORY_CMD_NAME, CliConsts.RUN_JHS_CMD_NAME)));
        System.exit(1);
        break;
    }
  }

  private static void checkAndPrintHelp(String[] args, CliParser parser) {
    if (HelpParser.helpSpecified(args)) {
      parser.printHelp();
      System.exit(1);
    }
  }

  private static void runSubmit(String[] args) throws IOException, URISyntaxException {

    CliParser<SpydraArgument> parser = new SubmissionCliParser();
    checkAndPrintHelp(args, parser);

    SpydraArgument userArguments = parser.parse(args);
    String userId = userId(SpydraArgumentUtil.isOnPremiseInvocation(userArguments));
    SpydraArgument finalArguments =
        SpydraArgumentUtil.mergeConfigurations(userArguments, userId);
    SpydraArgumentUtil.setDefaultClientIdIfRequired(finalArguments);
    SpydraArgumentUtil.setProjectFromCredentialsIfNotSet(finalArguments);
    finalArguments.replacePlaceholders();

    MetricsFactory.initialize(finalArguments, userId);
    Metrics metrics = MetricsFactory.getInstance();

    Submitter submitter = Submitter.getSubmitter(finalArguments);

    LOGGER.info("Executing submission command");
    boolean status = submitter.executeJob(finalArguments);

    metrics.executionResult(finalArguments, status);
    metrics.flush();
    System.exit(status ? 0 : 1);
  }

  private static String userId(boolean onPremiseInvocation) throws IOException {
    if (onPremiseInvocation) {
      return Optional.ofNullable(System.getenv("HADOOP_USER_NAME")).orElse("onpremise");
    } else {
      return gcpUtils.userIdFromJsonCredential(gcpUtils.credentialJsonFromEnv()).orElseThrow(
          () -> new IllegalArgumentException(
              "No valid credentials (service account) were available to forward to the cluster."));
    }
  }

  private static void runHistoryServer(String[] args) throws IOException {
    CliParser<RunJHSCommand> parser = new RunJHSCliParser();
    checkAndPrintHelp(args, parser);

    RunJHSCommand runJhsCommand = parser.parse(args);
    Configuration configuration = HistoryLogUtils.generateHadoopConfig(
        runJhsCommand.clientId(), runJhsCommand.logBucket());
    gcpUtils.configureCredentialFromEnvironment(configuration);
    HistoryLogUtils.startJHS(configuration);
  }

  private static void runDumpHistory(String[] args) throws IOException {
    CliParser<DumpHistoryCommand> parser = new DumpHistoryCliParser();
    checkAndPrintHelp(args, parser);

    DumpHistoryCommand dumpHistoryCommand = parser.parse(args);
    Configuration configuration = HistoryLogUtils.generateHadoopConfig(
        dumpHistoryCommand.clientId(),
        dumpHistoryCommand.logBucket());
    gcpUtils.configureCredentialFromEnvironment(configuration);
    HistoryLogUtils
        .dumpFullHistory(configuration,
            dumpHistoryCommand.applicationId());
  }

  private static void runDumpLogs(String[] args) throws IOException {
    CliParser<DumpLogsCommand> parser = new DumpLogsCliParser();
    checkAndPrintHelp(args, parser);

    DumpLogsCommand dumpLogsCommand = parser.parse(args);
    Configuration configuration = HistoryLogUtils.generateHadoopConfig(dumpLogsCommand.clientId(),
        dumpLogsCommand.username(), dumpLogsCommand.logBucket());
    gcpUtils.configureCredentialFromEnvironment(configuration);
    HistoryLogUtils.dumpFullLogs(configuration, dumpLogsCommand.applicationId());
  }
}
