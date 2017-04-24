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

import com.spotify.spydra.model.SpydraArgument;

import org.junit.Test;

import java.io.IOException;

import static com.spotify.spydra.submitter.runner.CliConsts.JOBNAME_OPTION_NAME;
import static org.junit.Assert.assertEquals;

public class SubmissionCliParserTest {

  public SpydraArgument testJobNameParameter(String providedId) throws IOException {
    SubmissionCliParser submissionCliParser = new SubmissionCliParser();

    String[] args = {"--" + JOBNAME_OPTION_NAME, providedId};

    return submissionCliParser.parse(args);
  }

  @Test
  public void testJobIdCharSet() throws IOException {

    String badId = "Some%id#using:several/disallowed*characters!";

    SpydraArgument args = testJobNameParameter(badId);
    String fixed_id = args.submit.getOptions().get(SpydraArgument.OPTION_JOB_ID);

    assertEquals("Some_id_using_several_disallowed_characters_", fixed_id.substring(0, badId.length()));
  }

  @Test
  public void testJobIdMaxLength() throws IOException {

    String tooLongId =
        "BadgerBadgerBadgerBadgerBadgerBadgerBadgerBadgerBadgerBadgerBadgerMushroomMushroom_SNAAAAAAAAAAAAAAAAAAKE";

    SpydraArgument args = testJobNameParameter(tooLongId);
    String fixed_id = args.submit.getOptions().get(SpydraArgument.OPTION_JOB_ID);

    assertEquals(88, fixed_id.length());
  }

}
