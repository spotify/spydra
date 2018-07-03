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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class HistoryLogUtilsTest {

  private Configuration configWithoutSubstitute;
  private final static long DUMMY_ID_TIMESTAMP = 123456789212L;
  private final static int DUMMY_ID_SERIAL = 1111;
  private final static String DUMMY_JOB_ID =
      "job_" + DUMMY_ID_TIMESTAMP + "_" + DUMMY_ID_SERIAL;
  private final static String DUMMY_JHIST_NAME = DUMMY_JOB_ID + "-fake-job-name.jhist";
  private final static String DUMMY_CLIENT_ID = "such-client";
  private final static String DUMMY_USER_NAME = "much-wow";
  private static final String DUMMY_BUCKET = "such-bucket";

  @Before
  public void setUp() {
    this.configWithoutSubstitute = new Configuration(false);
    this.configWithoutSubstitute.addResource("history.xml");
  }

  @Test
  public void testGenerateHadoopConfig() throws Exception {
    Configuration cfg = HistoryLogUtils.generateHadoopConfig(DUMMY_CLIENT_ID,
        DUMMY_USER_NAME, DUMMY_BUCKET);

    // We are asserting that the properties involving substitution have been changed
    checkPropertySubstitution(this.configWithoutSubstitute, cfg,
        YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        "gs://" + DUMMY_BUCKET + "/logs/such-client");

    checkPropertySubstitution(this.configWithoutSubstitute, cfg,
        JHAdminConfig.MR_HISTORY_INTERMEDIATE_DONE_DIR,
        "gs://" + DUMMY_BUCKET + "/history/such-client/done-intermediate");

    checkPropertySubstitution(this.configWithoutSubstitute, cfg,
        JHAdminConfig.MR_HISTORY_DONE_DIR,
        "gs://" + DUMMY_BUCKET + "/history/such-client/done");

    // Some additional guards to check whether we accidentally load additional config
    assertEquals("Sizes of configuration must not differ. Except for the user, client-id and bucket properties",
        cfg.size(), this.configWithoutSubstitute.size() + 3);
  }

  @Test
  public void testFindHistoryFilePath() throws Exception {

    final Iterator<LocatedFileStatus> mockListing = Arrays.asList(
        mockFileStatus("/foobar.jhist"),
        mockFileStatus("/barbaz.jhist"),
        mockFileStatus("/a.log"),
        mockFileStatus("/" + DUMMY_JHIST_NAME))
        .iterator();

    Optional<String> jHistFile = HistoryLogUtils.findHistoryFilePath(mockListing,
        ApplicationId.newInstance(DUMMY_ID_TIMESTAMP, DUMMY_ID_SERIAL));

    assertTrue(jHistFile.isPresent());
    assertEquals("/" + DUMMY_JHIST_NAME, jHistFile.get());
  }

  @Test
  public void testNoHistoryFound() throws Exception {

    final Iterator<LocatedFileStatus> mockListing = Arrays.asList(mockFileStatus("/a.log")).iterator();

    Optional<String> jHistFile = HistoryLogUtils.findHistoryFilePath(mockListing,
        ApplicationId.newInstance(DUMMY_ID_TIMESTAMP, DUMMY_ID_SERIAL));

    assertFalse(jHistFile.isPresent());
  }

  /**
   * Helper method for comparing that a property from one config (old) is different in another
   * config (new) and that said value in new config is expected. This is to test variable
   * substitution.
   */
  private void checkPropertySubstitution(Configuration oldConfig,
      Configuration newConfig, String propertyName, String expectedValue) {

    assertNotEquals("Property values must not be equal before and after substitution",
        newConfig.get(propertyName), oldConfig.get(propertyName));
    assertEquals("Substitution must have happened", expectedValue, newConfig.get(propertyName));
  }

  private LocatedFileStatus mockFileStatus(String filePath) {
    LocatedFileStatus mockLFS = Mockito.mock(LocatedFileStatus.class);
    Mockito.when(mockLFS.getPath()).thenReturn(new Path(filePath));
    return mockLFS;
  }

}
