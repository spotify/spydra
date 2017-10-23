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

import static java.lang.Integer.min;

import com.spotify.spydra.model.JsonHelper;
import com.spotify.spydra.model.SpydraArgument;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;

public class SubmissionCliParser implements CliParser<SpydraArgument> {
  final static String CMD_NAME = "submit";
  private final static Options options = new Options();

  public SubmissionCliParser() {
    options.addOption("n", SpydraArgument.OPTION_DRYRUN, false,
        "Do a dry run without executing anything");
    options.addOption(CliHelper.createSingleOption(CliConsts.SPYDRA_JSON_OPTION_NAME,
        "path to the spydra configuration json"));
    options.addOption(CliHelper.createSingleOption(CliConsts.CLIENT_ID_OPTION_NAME,
        "client id, overwrites the configured one if set"));
    options.addOption(CliHelper.createSingleOption(CliConsts.JAR_OPTION_NAME,
        "main jar path, overwrites the configured one if set"));
    options.addOption(CliHelper.createMultiOption(CliConsts.JARS_OPTION_NAME,
        "jar files to be shipped with the job, can occur multiple times" +
            ", overwrites the configured ones if set"));
    options.addOption(CliHelper.createSingleOption(CliConsts.JOBNAME_OPTION_NAME,
        "job name, used as dataproc job id"));
    options.addOption(CliHelper.createMultiOption(CliConsts.LABELS_OPTION_NAME,
        "labels to apply to the cluster and/or job, can occur multiple times" +
            " to add additional labels"));
  }

  public SpydraArgument parse(String[] args) throws IOException {
    DefaultParser parser = new DefaultParser();
    CommandLine cmdLine;

    cmdLine = CliHelper.tryParse(parser, options, args);

    SpydraArgument spydraArgument;
    if (cmdLine.hasOption(CliConsts.SPYDRA_JSON_OPTION_NAME)) {
      FileInputStream f = new FileInputStream(cmdLine.getOptionValue(
          CliConsts.SPYDRA_JSON_OPTION_NAME));
      spydraArgument = JsonHelper.fromStream(f, SpydraArgument.class);
    } else {
      spydraArgument = new SpydraArgument();
    }

    if (cmdLine.hasOption(CliConsts.JAR_OPTION_NAME)) {
      spydraArgument.getSubmit().getOptions().put(SpydraArgument.OPTION_JAR,
          cmdLine.getOptionValue(CliConsts.JAR_OPTION_NAME));
    }

    if (cmdLine.hasOption(CliConsts.CLIENT_ID_OPTION_NAME)) {
      spydraArgument.setClientId(cmdLine.getOptionValue(CliConsts.CLIENT_ID_OPTION_NAME));
    }

    if (cmdLine.hasOption(CliConsts.JARS_OPTION_NAME)) {
      spydraArgument.getSubmit().getOptions().put(SpydraArgument.OPTION_JARS,
          StringUtils.join(cmdLine.getOptionValues(CliConsts.JARS_OPTION_NAME), ","));
    }

    if (cmdLine.hasOption(CliConsts.JOBNAME_OPTION_NAME)) {
      spydraArgument.getSubmit().getOptions().put(SpydraArgument.OPTION_JOB_ID,
          sanitizeJobId(cmdLine.getOptionValue(CliConsts.JOBNAME_OPTION_NAME)));
    }

    if (cmdLine.hasOption(CliConsts.LABELS_OPTION_NAME)) {
      String labels = StringUtils.join(cmdLine.getOptionValues(CliConsts.LABELS_OPTION_NAME), ",");
      spydraArgument.getSubmit().getOptions().compute(SpydraArgument.OPTION_LABELS,
          (key, existingLabels) -> (existingLabels == null) ? labels : existingLabels + ',' + labels);
      spydraArgument.getCluster().getOptions().compute(SpydraArgument.OPTION_LABELS,
          (key, existingLabels) -> (existingLabels == null) ? labels : existingLabels + ',' + labels);
    }

    if (spydraArgument.jobType.isPresent()) {
      spydraArgument.setJobType(spydraArgument.getJobType().toLowerCase());
    }

    if (cmdLine.hasOption(SpydraArgument.OPTION_DRYRUN)) {
      spydraArgument.setDryRun(true);
    }

    List<String> jobArgs = new LinkedList<>(cmdLine.getArgList());
    if (jobArgs.size() > 0) {
      spydraArgument.getSubmit().setJobArgs(jobArgs);
    }

    return spydraArgument;
  }

  @Override
  public boolean enoughArgs(String[] args) {
    return args.length <= 1;
  }

  public void printHelp() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(CMD_NAME + " [jobArgs]", options);
  }

  private String sanitizeJobId(String jobID) {

    String uuid = UUID.randomUUID().toString();

    //Limit size so that id with uuid is max 88 characters
    //Specs say 100, but Google suspect they have issues >88
    int maxLength = min(88 - uuid.length() - 1, jobID.length());
    jobID = jobID.substring(0, maxLength);

    //Remove all non-allowed characters
    jobID = jobID.replaceAll("[^a-zA-Z0-9_-]", "_");

    //Append uuid
    jobID = jobID + "-" + uuid;

    return jobID;
  }
}
