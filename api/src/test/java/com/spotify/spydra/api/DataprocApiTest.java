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

package com.spotify.spydra.api;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.spotify.spydra.api.gcloud.GcloudExecutor;
import com.spotify.spydra.api.model.Job;
import com.spotify.spydra.api.process.ProcessResult;
import com.spotify.spydra.api.process.ProcessService;
import com.spotify.spydra.metrics.MetricsFactory;
import com.spotify.spydra.model.SpydraArgument;
import com.spotify.spydra.model.SpydraArgument.Cluster;
import com.spotify.spydra.model.SpydraArgument.Submit;

import static com.spotify.spydra.api.TestUtils.*;

import static org.mockito.Mockito.*;

import static org.junit.Assert.*;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

public class DataprocApiTest {

  private DataprocApi api;
  private GcloudExecutor gcloudExecutor;
  private ProcessService processService;
  private SpydraArgument arguments;

  @Before
  public void setUp() {
    processService = mock(ProcessService.class);
    gcloudExecutor = new GcloudExecutor(processService);
    api = new DataprocApi(gcloudExecutor, MetricsFactory.getInstance(), Clock.systemUTC());
    arguments = spydraArguments("example-project", "us-central1");
  }

  @Test
  public void testFindJobToResume() throws Exception {

    ArgumentCaptor<List<String>> commandCaptor = listArgumentCaptor(String.class);

    when(processService.executeForOutput(commandCaptor.capture()))
      .thenReturn(new ProcessResult(0, fromResource("/job-single.json")));

    Optional<Job> maybeJob = api.findJobToResume(arguments);

    assertTrue(maybeJob.isPresent());

    List<String> command = commandCaptor.getValue();

    assertTrue(command.contains("--region=us-central1"));
    assertTrue(command.contains("--project=example-project"));
    assertTrue(command.contains("--filter=labels.spydra-dedup-id=job1"));
    assertTrue(command.contains("--limit=1"));
    assertTrue(command.contains("--sort-by=~status.stateStartTime"));
  }

  @Test
  public void testFIndJobToResumeWithoutTimeLimit() throws Exception {

    when(processService.executeForOutput(anyListOf(String.class)))
      .thenReturn(new ProcessResult(0, fromResource("/job-single.json")));

    Optional<Job> maybeJob = api.findJobToResume(arguments);

    assertTrue(maybeJob.isPresent());
  }

  @Test
  public void testJobToResumeIsWithinTimeLimit() throws Exception {

    Instant currentTime = Instant.parse("2018-08-30T12:00:00.000Z");
    api = new DataprocApi(gcloudExecutor, MetricsFactory.getInstance(), Clock.fixed(currentTime, ZoneOffset.UTC));

    when(processService.executeForOutput(anyListOf(String.class)))
      .thenReturn(new ProcessResult(0, fromResource("/job-single.json")));

    arguments.deduplicationMaxAge = Optional.of(Duration.of(6, ChronoUnit.HOURS).getSeconds());

    Optional<Job> maybeJob = api.findJobToResume(arguments);

    assertTrue(maybeJob.isPresent());
  }

  @Test
  public void testJobToResumeIsTooOld() throws Exception {

    Instant currentTime = Instant.parse("2018-08-30T13:00:00.000Z");
    api = new DataprocApi(gcloudExecutor, MetricsFactory.getInstance(), Clock.fixed(currentTime, ZoneOffset.UTC));

    when(processService.executeForOutput(anyListOf(String.class)))
      .thenReturn(new ProcessResult(0, fromResource("/job-single.json")));

    arguments.deduplicationMaxAge = Optional.of(Duration.of(6, ChronoUnit.HOURS).getSeconds());

    Optional<Job> maybeJob = api.findJobToResume(arguments);

    assertFalse(maybeJob.isPresent());
  }

  private SpydraArgument spydraArguments(String project, String region) {
    SpydraArgument arguments = new SpydraArgument();

    Cluster cluster = arguments.new Cluster();
    cluster.project(project);
    arguments.setCluster(cluster);

    Submit submit = arguments.new Submit();
    submit.setLabels(SpydraArgument.OPTIONS_DEDUPLICATING_LABEL + "=job1");
    arguments.setSubmit(submit);

    arguments.setRegion(region);
    return arguments;
  }
}
