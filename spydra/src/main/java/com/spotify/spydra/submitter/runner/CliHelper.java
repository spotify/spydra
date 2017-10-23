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
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helpful utilities to handle cli parsing and options creation
 */
public class CliHelper {
  static final Logger logger = LoggerFactory.getLogger(CliHelper.class);

  /**
   * Creates an optional long parameter with a single argument
   */
  public static Option createSingleOption(String optName, String description) {
    return createOption(optName, description, false, false);
  }

  /**
   * Creates a required long parameter with a single argument
   */
  public static Option createRequiredSingleOption(String optName, String description) {
    return createOption(optName, description, true, false);
  }

  /**
   * Creates an optional long parameter with multiple arguments
   */
  public static Option createMultiOption(String optName, String description) {
    return createOption(optName, description, false, true);
  }

  /**
   * Small helper for creating some of the long parameter settings we frequently
   * use here.
   *
   * @param optName       name of the parameter/option
   * @param description   description for the help text
   * @param isRequired    whether this parameter is strictly required
   * @param unlimitedArgs whether it supports multiple arguments, default is on argument
   */
  public static Option createOption(String optName, String description,
      boolean isRequired, boolean unlimitedArgs) {

    Option.Builder builder = Option.builder()
        .longOpt(optName)
        .hasArg()
        .valueSeparator(',')
        .required(isRequired)
        .desc(description);

    if (unlimitedArgs) {
      builder.hasArgs();
    }

    return builder.build();
  }

  public static CommandLine tryParse(CommandLineParser parser, Options options,
      String[] args) {

    try {
      return parser.parse(options, args);
    } catch (MissingOptionException moe) {
      logger.error("Required options missing: " + Joiner.on(',').join(moe.getMissingOptions()));
      throw new CliParser.ParsingException(moe);
    } catch (ParseException e) {
      logger.error("Failed parsing options", e);
      throw new CliParser.ParsingException(e);
    }
  }
}
