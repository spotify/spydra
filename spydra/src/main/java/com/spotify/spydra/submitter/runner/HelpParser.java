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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.LoggerFactory;

/**
 * Small helper to parse for help-parameters for different subcommands
 */
public class HelpParser {
  final static String HELP_OPTION_NAME = "help";
  static final org.slf4j.Logger logger = LoggerFactory.getLogger(Runner.class);

  private static final Options options;
  private static final DefaultParser parser;

  static {
    options = new Options();
    HelpParser.options.addOption(Option.builder("h")
        .longOpt(HELP_OPTION_NAME)
        .hasArg(false)
        .required(false)
        .desc("this")
        .build());
    parser = new DefaultParser();
  }

  /**
   * Checks whether -h or --help is specified in given arguments. Ignores
   * all unknown arguments.
   *
   * @param args cli arguments
   * @return whether or not help is specified
   */
  public static boolean helpSpecified(String[] args) {
    if (args.length < 1) {
      return false;
    }

    try {
      CommandLine commandLine = HelpParser.parser.parse(HelpParser.options, args, true);
      return commandLine.hasOption(HELP_OPTION_NAME);
    } catch (ParseException e) {
      logger.error("Failed parsing for existence of --help", e);
      return false;
    }
  }
}
