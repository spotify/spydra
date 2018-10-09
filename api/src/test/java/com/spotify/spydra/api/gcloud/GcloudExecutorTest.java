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

package com.spotify.spydra.api.gcloud;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.spotify.spydra.api.model.Job;
import com.spotify.spydra.api.process.ProcessResult;
import com.spotify.spydra.api.process.ProcessService;

import static com.spotify.spydra.api.TestUtils.*;

import static org.mockito.Mockito.*;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class GcloudExecutorTest {

  private GcloudExecutor gcloudExecutor;
  private ProcessService processService;

  @Before
  public void setUp() {
    processService = mock(ProcessService.class);
    gcloudExecutor = new GcloudExecutor(processService);
  }

  @Test
  public void testListJobsWithOptinalArgs() throws IOException {

    ArgumentCaptor<List<String>> commandCaptor = listArgumentCaptor(String.class);

    when(processService.executeForOutput(commandCaptor.capture()))
      .thenReturn(new ProcessResult(0, fromResource("/job-list.json")));

    List<Job> jobs = gcloudExecutor.listJobs("<project>", "<region>",
        Collections.singletonMap("<filterkey>", "<filtervalue>"),
        Optional.of(99),
        Optional.of("<sortby>"));

    assertEquals(2,  jobs.size());

    List<String> command = commandCaptor.getValue();
    assertTrue(command.contains("--region=<region>"));
    assertTrue(command.contains("--project=<project>"));
    assertTrue(command.contains("--filter=<filterkey>=<filtervalue>"));
    assertTrue(command.contains("--limit=99"));
    assertTrue(command.contains("--sort-by=<sortby>"));
  }

  @Test
  public void testListJobsWithoutOptionalArgs() throws IOException {

    ArgumentCaptor<List<String>> commandCaptor = listArgumentCaptor(String.class);

    when(processService.executeForOutput(commandCaptor.capture()))
      .thenReturn(new ProcessResult(0, fromResource("/job-list.json")));

    List<Job> jobs = gcloudExecutor.listJobs("<project>", "<region>",
        Collections.emptyMap(),
        Optional.empty(),
        Optional.empty());

    assertEquals(2,  jobs.size());

    List<String> command = commandCaptor.getValue();
    assertTrue(command.contains("--region=<region>"));
    assertTrue(command.contains("--project=<project>"));
    assertNoneStartsWith(command, "--filter");
    assertNoneStartsWith(command, "--limit");
    assertNoneStartsWith(command, "--sort-by");
  }
}


