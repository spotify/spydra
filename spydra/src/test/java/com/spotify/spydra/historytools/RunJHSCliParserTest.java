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

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;
import com.spotify.spydra.CliTestHelpers;
import com.spotify.spydra.historytools.commands.RunJHSCommand;
import com.spotify.spydra.submitter.runner.CliConsts;
import com.spotify.spydra.submitter.runner.CliParser;
import org.junit.Test;

public class RunJHSCliParserTest {

  private static final RunJHSCliParser PARSER = new RunJHSCliParser();
  private static final String DUMMY_CLIENT_ID = "pretty-client-2342";
  private static final String DUMMY_BUCKET = "bucket";

  @Test
  public void testParseWithUser() {
    RunJHSCommand jhsCmd = PARSER.parse(new String[]{
        CliTestHelpers.toStrOpt(CliConsts.CLIENT_ID_OPTION_NAME, DUMMY_CLIENT_ID),
        CliTestHelpers.toStrOpt(CliConsts.LOG_BUCKET_OPTION_NAME, DUMMY_BUCKET)
    });

    assertEquals(jhsCmd.clientId(), DUMMY_CLIENT_ID);
  }

  @Test
  public void testParseWithoutUser() {
    RunJHSCommand jhsCmd = PARSER.parse(new String[]{
        CliTestHelpers.toStrOpt(CliConsts.CLIENT_ID_OPTION_NAME, DUMMY_CLIENT_ID),
        CliTestHelpers.toStrOpt(CliConsts.LOG_BUCKET_OPTION_NAME, DUMMY_BUCKET)
    });

    assertEquals(jhsCmd.clientId(), DUMMY_CLIENT_ID);
  }

  @Test
  public void testMissingArgs() {
    CliTestHelpers.ensureAllThrow(PARSER, Lists.newArrayList(
        new String[]{},
        new String[]{"--noSuchThing=FOO"}
        ),
        CliParser.ParsingException.class);
  }
}
