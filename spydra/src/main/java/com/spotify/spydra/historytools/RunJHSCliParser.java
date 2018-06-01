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

package com.spotify.spydra.historytools;

import com.spotify.spydra.historytools.commands.RunJHSCommand;
import com.spotify.spydra.submitter.runner.CliConsts;
import com.spotify.spydra.submitter.runner.CliHelper;
import com.spotify.spydra.submitter.runner.CliParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

public class RunJHSCliParser implements CliParser<RunJHSCommand> {
  public final static String CMD_NAME = CliConsts.RUN_JHS_CMD_NAME;

  private static final Options options;

  static {
    options = buildCliOptions();
  }

  @Override
  public RunJHSCommand parse(String[] args) {
    DefaultParser parser = new DefaultParser();
    CommandLine cmdLine;

    cmdLine = CliHelper.tryParse(parser, options, args);

    return RunJHSCommand.builder()
        .clientId(cmdLine.getOptionValue(CliConsts.CLIENT_ID_OPTION_NAME))
        .logBucket(cmdLine.getOptionValue(CliConsts.LOG_BUCKET_OPTION_NAME))
        .build();
  }

  private static Options buildCliOptions() {
    Options options = new Options();

    options.addOption(CliHelper.createRequiredSingleOption(CliConsts.CLIENT_ID_OPTION_NAME,
        "client-id used for cluster lifetime"));
    options.addOption(CliHelper.createSingleOption(CliConsts.USERNAME_OPTION_NAME,
        "user that ran the job - optional but required to view logs"));
    options.addOption(CliHelper.createRequiredSingleOption(CliConsts.LOG_BUCKET_OPTION_NAME,
        " name of the bucket storing the Hadoop logs and history information"));

    return options;
  }

  @Override
  public void printHelp() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.setLongOptSeparator("=");
    formatter.printHelp(CMD_NAME, RunJHSCliParser.options);
  }

  @Override
  public boolean enoughArgs(String[] args) {
    return args.length > 0;
  }

}
