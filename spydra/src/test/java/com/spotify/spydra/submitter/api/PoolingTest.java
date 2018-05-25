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

package com.spotify.spydra.submitter.api;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.spydra.api.DataprocAPI;
import com.spotify.spydra.api.model.Cluster;
import com.spotify.spydra.model.SpydraArgument;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

@RunWith(Enclosed.class)
public class PoolingTest {

  private static final String spydraClusterName = "spydra-uuid";
  private static final String nonSpydraClusterName = "not-a-spydra-cluster";

  static Cluster perfectCluster(String clientid) {
    Cluster cluster = new Cluster();
    cluster.clusterName = spydraClusterName;
    Cluster.Status status = new Cluster.Status();
    status.state = Cluster.Status.RUNNING;
    status.stateStartTime = ZonedDateTime.now(ZoneOffset.UTC);
    cluster.status = status;
    cluster.labels = ImmutableMap.of(DynamicSubmitter.SPYDRA_CLUSTER_LABEL, "1",
        PoolingSubmitter.POOLED_CLUSTER_CLIENTID_LABEL, clientid);
    cluster.config.gceClusterConfig.metadata.heartbeat =
        Optional.of(cluster.status.stateStartTime.plusMinutes(60));
    return cluster;
  }

  static Cluster nonSpydraCluster(String clientid) {
    Cluster cluster = new Cluster();
    cluster.labels = ImmutableMap.of();
    cluster.clusterName = "not-a-spydra-cluster";
    Cluster.Status status = new Cluster.Status();
    cluster.labels = ImmutableMap.of(DynamicSubmitter.SPYDRA_CLUSTER_LABEL, "1",
        PoolingSubmitter.POOLED_CLUSTER_CLIENTID_LABEL, clientid);
    status.state = Cluster.Status.RUNNING;
    status.stateStartTime = ZonedDateTime.now(ZoneOffset.UTC);
    return cluster;
  }

  static Cluster creatingCluster(String clientid) {
    Cluster cluster = perfectCluster(clientid);
    cluster.status.state = "CREATING";
    return cluster;
  }

  static Cluster ancientCluster(String clientid) {
    Cluster cluster = perfectCluster(clientid);

    //Make cluster an hour older, and last heartbeat 30 minutes ago
    cluster.status.stateStartTime = cluster.status.stateStartTime.minusMinutes(60);
    cluster.config.gceClusterConfig.metadata.heartbeat =
        Optional.of(cluster.status.stateStartTime.minusMinutes(30));
    return cluster;
  }

  static Cluster errorCluster(String clientid) {
    Cluster cluster = perfectCluster(clientid);
    cluster.status.state = Cluster.Status.ERROR;
    return cluster;
  }

  public static class PoolingSubmitterTest {

    PoolingSubmitter poolingSubmitter;
    DataprocAPI dataprocAPI;
    SpydraArgument arguments;
    String clientId;

    @Before
    public void before() {
      poolingSubmitter = new PoolingSubmitter(Clock.systemUTC()::millis, "account");
      dataprocAPI = mock(DataprocAPI.class);
      arguments = new SpydraArgument();
      clientId = "my-client-id";
    }

    @Test
    public void acquirePooledCluster() throws Exception {

      ImmutableList<Cluster> clusters =
          ImmutableList.of(perfectCluster(clientId), perfectCluster(clientId));

      SpydraArgument.Pooling pooling = new SpydraArgument.Pooling();
      pooling.setLimit(2); // 2 perfectly suited clusters above!
      pooling.setMaxAge(Duration.ofMinutes(30));
      arguments.setClientId(clientId);
      arguments.setPooling(pooling);

      when(dataprocAPI.listClusters(eq(arguments), anyMap()))
          .thenReturn(clusters);
      when(dataprocAPI.createCluster(arguments))
          .thenReturn(Optional.of(new Cluster()));

      boolean result = poolingSubmitter.acquireCluster(arguments, dataprocAPI);
      assertTrue("Failed to acquire a cluster", result);
      verify(dataprocAPI, never()).createCluster(arguments);

      assertTrue("A healthy cluster should be kept without problems.",
          poolingSubmitter.releaseCluster(arguments, dataprocAPI));
      verify(dataprocAPI, never()).deleteCluster(arguments);

    }

    @Test
    public void avoidAncientCluster() throws Exception {
      ImmutableList<Cluster> clusters =
          ImmutableList.of(ancientCluster(clientId), ancientCluster(clientId));

      SpydraArgument.Pooling pooling = new SpydraArgument.Pooling();
      pooling.setLimit(2); // 2 reasonable but old clusters above. Pool is "full", but none usable.
      pooling.setMaxAge(Duration.ofMinutes(30));
      arguments.setClientId(clientId);
      arguments.setPooling(pooling);

      when(dataprocAPI.listClusters(eq(arguments), anyMap()))
          .thenReturn(clusters);
      when(dataprocAPI.createCluster(arguments))
          .thenReturn(Optional.of(new Cluster()));

      boolean result = poolingSubmitter.acquireCluster(arguments, dataprocAPI);
      assertTrue("Failed to acquire a cluster", result);
      verify(dataprocAPI, times(1)).createCluster(arguments);
    }

    @Test
    public void acquireNewCluster() throws Exception {
      ImmutableList<Cluster> clusters =
          ImmutableList.of(perfectCluster(clientId), perfectCluster(clientId));

      SpydraArgument.Pooling pooling = new SpydraArgument.Pooling();
      pooling.setLimit(3); // 2 perfectly suited clusters above, so room for 1 more!
      pooling.setMaxAge(Duration.ofMinutes(30));
      arguments.setPooling(pooling);
      arguments.setClientId(clientId);

      when(dataprocAPI.listClusters(eq(arguments), anyMap()))
          .thenReturn(clusters);
      when(dataprocAPI.createCluster(arguments))
          .thenReturn(Optional.of(new Cluster()));
      boolean result = poolingSubmitter.acquireCluster(arguments, dataprocAPI);
      assertTrue("Failed to acquire a cluster", result);
      verify(dataprocAPI, times(1)).createCluster(arguments);
    }

    @Test
    public void releaseCluster() throws Exception {
      ImmutableList<Cluster> clusters =
          ImmutableList.of(perfectCluster(clientId), perfectCluster(clientId));

      SpydraArgument.Pooling pooling = new SpydraArgument.Pooling();
      arguments.setPooling(pooling);
      arguments.setClientId(clientId);
      arguments.cluster.setName(spydraClusterName);

      when(dataprocAPI.listClusters(eq(arguments), anyMap()))
          .thenReturn(clusters);
      assertTrue("A healthy cluster should be kept without problems.",
          poolingSubmitter.releaseCluster(arguments, dataprocAPI));
      verify(dataprocAPI).listClusters(eq(arguments), anyMap());
      verify(dataprocAPI, never()).deleteCluster(arguments);
    }

    @Test
    public void releaseErrorCluster() throws Exception {
      ImmutableList<Cluster> clusters =
          ImmutableList.of(errorCluster(clientId), errorCluster(clientId));

      SpydraArgument.Pooling pooling = new SpydraArgument.Pooling();
      arguments.setPooling(pooling);
      arguments.setClientId(clientId);
      arguments.cluster.setName(spydraClusterName);

      when(dataprocAPI.listClusters(eq(arguments), anyMap()))
          .thenReturn(clusters);
      when(dataprocAPI.deleteCluster(arguments)).thenReturn(true);
      assertTrue("A broken cluster should be collected without problems.",
          poolingSubmitter.releaseCluster(arguments, dataprocAPI));
      verify(dataprocAPI).listClusters(eq(arguments), anyMap());
      verify(dataprocAPI).deleteCluster(arguments);
    }
  }
}
