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
package com.spotify.spydra.submitter.api;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.spotify.spydra.api.DataprocApi;
import com.spotify.spydra.api.model.Cluster;
import com.spotify.spydra.api.model.Job;
import com.spotify.spydra.api.model.Job.Status;
import com.spotify.spydra.api.model.Job.Reference;
import com.spotify.spydra.model.ClusterType;
import com.spotify.spydra.model.SpydraArgument;
import com.spotify.spydra.util.GcpUtils;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

/**
 * Created by TwN on 2017-04-20.
 */
public class DynamicSubmitterTest {

  private static final String spydraClusterName = "spydra-uuid";

  DynamicSubmitter dynamicSubmitter;
  DataprocApi dataprocApi;
  GcpUtils gcpUtils;
  SpydraArgument arguments;
  String clientId;

  @Before
  public void before() {
    dataprocApi = mock(DataprocApi.class);
    gcpUtils = mock(GcpUtils.class);
    dynamicSubmitter = new DynamicSubmitter(dataprocApi, gcpUtils);
    arguments = new SpydraArgument();
    clientId = "my-client-id";
  }


  @Test
  public void releaseErrorCluster() throws Exception {
    List<Cluster> clusters = Arrays.asList(errorCluster(), errorCluster());

    SpydraArgument.Pooling pooling = new SpydraArgument.Pooling();
    arguments.setPooling(pooling);
    arguments.setClientId(clientId);
    arguments.cluster.setName(spydraClusterName);

    when(dataprocApi.listClusters(eq(arguments), anyMapOf(String.class, String.class)))
            .thenReturn(clusters);
    when(dataprocApi.deleteCluster(arguments)).thenReturn(true);
    assertTrue("A broken cluster should be collected without problems.",
        dynamicSubmitter.releaseCluster(arguments, dataprocApi));
    verify(dataprocApi).deleteCluster(arguments);
  }

  private static Cluster perfectCluster() {
    Cluster cluster = new Cluster();
    cluster.clusterName = spydraClusterName;
    Cluster.Status status = new Cluster.Status();
    status.state = Cluster.Status.RUNNING;
    status.stateStartTime = ZonedDateTime.now(ZoneOffset.UTC);
    cluster.status = status;
    cluster.labels = Collections.singletonMap(DynamicSubmitter.SPYDRA_CLUSTER_LABEL, "1");
    return cluster;
  }

  private static Cluster errorCluster() {
    Cluster cluster = perfectCluster();
    cluster.status.state = Cluster.Status.ERROR;
    return cluster;
  }

  @Test
  public void reattachToOriginalRunningJob() throws Exception {
    whenRunningDuplicateJobWaitForOriginal(new Status(Status.RUNNING));
  }

  @Test
  public void returnResultOfOriginalSucessfulJob() throws Exception {
    whenRunningDuplicateJobWaitForOriginal(new Status(Status.DONE));
  }

  private void whenRunningDuplicateJobWaitForOriginal(Status statusOfOriginal) throws Exception {
    SpydraArgument.Submit submit = arguments.new Submit();
    submit.setLabels("spydra-dedup-id=1");
    arguments.setSubmit(submit);

    Job existingJob = new Job(new Reference("jobid1"), statusOfOriginal);

    when(dataprocApi.findJobToResume(eq(arguments)))
      .thenReturn(Optional.of(existingJob));
    when(dataprocApi.waitJobForOutput(eq(arguments), eq(existingJob.reference.jobId))).thenReturn(true);

    boolean result = dynamicSubmitter.executeJob(arguments);

    assertTrue(result);

    verify(dataprocApi).findJobToResume(eq(arguments));
    verify(dataprocApi).waitJobForOutput(eq(arguments), eq(existingJob.reference.jobId));

    verify(dataprocApi, never()).createCluster(any(SpydraArgument.class));
    verify(dataprocApi, never()).deleteCluster(any(SpydraArgument.class));
  }

  @Test
  public void rerunDuplicateFailedJob() throws Exception {
    SpydraArgument.Submit submit = arguments.new Submit();
    submit.setLabels("spydra-dedup-id=1");
    arguments.setSubmit(submit);
    arguments.setClusterType(ClusterType.DATAPROC);

    Job existingJob = new Job(new Reference("jobid1"), new Status(Status.ERROR));

    when(dataprocApi.findJobToResume(eq(arguments)))
      .thenReturn(Optional.of(existingJob));

    when(dataprocApi.createCluster(eq(arguments))).thenReturn(Optional.of(new Cluster()));
    when(dataprocApi.submit(eq(arguments))).thenReturn(true);

    boolean result = dynamicSubmitter.executeJob(arguments);

    assertTrue(result);

    verify(dataprocApi).findJobToResume(eq(arguments));
    verify(dataprocApi, never()).waitJobForOutput(any(SpydraArgument.class), eq(existingJob.reference.jobId));

    verify(dataprocApi).createCluster(eq(arguments));
    verify(dataprocApi).submit(eq(arguments));
    verify(dataprocApi).deleteCluster(eq(arguments));
  }

  @Test
  public void reattachToJobThatFails() throws Exception {
    SpydraArgument.Submit submit = arguments.new Submit();
    submit.setLabels("spydra-dedup-id=1");
    arguments.setSubmit(submit);
    arguments.setClusterType(ClusterType.DATAPROC);

    Job existingJob = new Job(new Reference("jobid1"), new Status(Status.PENDING));

    when(dataprocApi.findJobToResume(eq(arguments)))
      .thenReturn(Optional.of(existingJob));

    when(dataprocApi.waitJobForOutput(eq(arguments), eq(existingJob.reference.jobId))).thenReturn(false);

    boolean result = dynamicSubmitter.executeJob(arguments);

    assertFalse(result);

    verify(dataprocApi).findJobToResume(eq(arguments));
    verify(dataprocApi).waitJobForOutput(eq(arguments), eq(existingJob.reference.jobId));

    verify(dataprocApi, never()).createCluster(eq(arguments));
    verify(dataprocApi, never()).submit(eq(arguments));
    verify(dataprocApi, never()).deleteCluster(eq(arguments));
  }

}
