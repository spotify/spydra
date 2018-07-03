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

import com.spotify.spydra.CliTestHelpers;
import com.spotify.spydra.historytools.commands.DumpLogsCommand;
import com.spotify.spydra.submitter.runner.CliConsts;
import com.spotify.spydra.submitter.runner.CliParser;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class DumpLogsCliParserTest {

  private static final DumpLogsCliParser PARSER = new DumpLogsCliParser();
  private static final String DUMMY_APP_ID = "application_1345678910111_123456";
  private static final String DUMMY_CLIENT_ID = "pretty-client-2342";
  private static final String DUMMY_USER = "rewt";
  private static final String DUMMY_BUCKET = "bucket";

  @Test
  public void testParse() {
    DumpLogsCommand dumpCmd = PARSER.parse(new String[]{
        CliTestHelpers.toStrOpt(CliConsts.JOB_ID_OPTION_NAME, DUMMY_APP_ID),
        CliTestHelpers.toStrOpt(CliConsts.CLIENT_ID_OPTION_NAME, DUMMY_CLIENT_ID),
        CliTestHelpers.toStrOpt(CliConsts.USERNAME_OPTION_NAME, DUMMY_USER),
        CliTestHelpers.toStrOpt(CliConsts.LOG_BUCKET_OPTION_NAME, DUMMY_BUCKET)
    });

    assertEquals(dumpCmd.applicationId().toString(), DUMMY_APP_ID);
    assertEquals(dumpCmd.clientId(), DUMMY_CLIENT_ID);
    assertEquals(dumpCmd.username(), DUMMY_USER);
    assertEquals(dumpCmd.logBucket(), DUMMY_BUCKET);
  }

  @Test
  public void testMissingArgs() {
    // Ensure options are mandatory
    List<String[]> failingLines = Arrays.asList(
        new String[]{
            CliTestHelpers.toStrOpt(CliConsts.CLIENT_ID_OPTION_NAME, DUMMY_CLIENT_ID)
        },
        new String[]{
            CliTestHelpers.toStrOpt(CliConsts.JOB_ID_OPTION_NAME, DUMMY_APP_ID)
        },
        new String[]{
            CliTestHelpers.toStrOpt(CliConsts.USERNAME_OPTION_NAME, DUMMY_USER)
        },
        new String[]{
            CliTestHelpers.toStrOpt(CliConsts.CLIENT_ID_OPTION_NAME, DUMMY_CLIENT_ID),
            CliTestHelpers.toStrOpt(CliConsts.JOB_ID_OPTION_NAME, DUMMY_APP_ID),
            CliTestHelpers.toStrOpt(CliConsts.USERNAME_OPTION_NAME, DUMMY_USER)
        }
    );

    CliTestHelpers.ensureAllThrow(PARSER, failingLines, CliParser.ParsingException.class);
  }
}
