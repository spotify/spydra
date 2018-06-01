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

package com.spotify.spydra.submitter.runner;

import static com.spotify.spydra.submitter.runner.CliConsts.JOBNAME_OPTION_NAME;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.spotify.spydra.model.JsonHelper;
import com.spotify.spydra.model.SpydraArgument;
import java.io.File;
import java.io.IOException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SubmissionCliParserTest {

  public @Rule TemporaryFolder temporaryFolder = new TemporaryFolder();

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

  @Test
  public void testMultipleSpydraJson() throws IOException {
    File jsonFile1 = temporaryFolder.newFile("spydra1.json");
    File jsonFile2 = temporaryFolder.newFile("spydra2.json");
    JsonHelper.objectMapper().writeValue(jsonFile1, ImmutableMap.of(
        "cluster", ImmutableMap.of("options", ImmutableMap.of("labels", "ck1=cv1")),
        "submit", ImmutableMap.of("options", ImmutableMap.of("labels", "sk1=sv1"))
    ));
    JsonHelper.objectMapper().writeValue(jsonFile2, ImmutableMap.of(
        "cluster", ImmutableMap.of("options", ImmutableMap.of("labels", "ck2=cv2")),
        "submit", ImmutableMap.of("options", ImmutableMap.of("labels", "sk2=sv2"))
    ));
    SubmissionCliParser parser = new SubmissionCliParser();
    String[] args = {
        "--spydra-json", jsonFile1.toString(),
        "--spydra-json", jsonFile2.toString(),
    };
    SpydraArgument argument = parser.parse(args);
    assertEquals("ck1=cv1,ck2=cv2", argument.cluster.options.get("labels"));
    assertEquals("sk1=sv1,sk2=sv2", argument.submit.options.get("labels"));
  }
}
