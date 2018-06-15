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

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.spydra.api.DataprocAPI;
import com.spotify.spydra.api.model.Cluster;
import com.spotify.spydra.model.SpydraArgument;
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
  private static final String VALID_TOKEN_1 = "0-0";
  private static final String VALID_TOKEN_2 = "1-0";
  private static final Long NOW = Duration.ofMinutes(30).getSeconds() * 1000;

  static Cluster perfectCluster(final String clientid,
                                final int clusterNumber,
                                final long generation) {
    Cluster cluster = new Cluster();
    cluster.clusterName = spydraClusterName;
    Cluster.Status status = new Cluster.Status();
    status.state = Cluster.Status.RUNNING;
    status.stateStartTime = ZonedDateTime.now(ZoneOffset.UTC);
    cluster.status = status;
    cluster.labels = ImmutableMap.of(DynamicSubmitter.SPYDRA_CLUSTER_LABEL, "1",
        PoolingSubmitter.POOLED_CLUSTER_CLIENTID_LABEL, clientid,
        PoolingSubmitter.SPYDRA_PLACEMENT_TOKEN_LABEL,
        String.format("%d-%d", clusterNumber, generation));
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

  static Cluster errorCluster(final String clientid,
                              final int clusterNumber,
                              final long generation) {
    Cluster cluster = perfectCluster(clientid, clusterNumber, generation);
    cluster.status.state = Cluster.Status.ERROR;
    return cluster;
  }

  public static class PoolingSubmitterTest {

    PoolingSubmitter poolingSubmitter;
    DataprocAPI dataprocAPI;
    SpydraArgument arguments;
    String clientId;
    private RandomPlacementGenerator randomPlacementGenerator;

    @Before
    public void before() {
      randomPlacementGenerator = mock(DefaultRandomPlacementGenerator.class);
      when(randomPlacementGenerator.randomPlacement(anyListOf(ClusterPlacement.class)))
          .thenCallRealMethod();
      poolingSubmitter = new PoolingSubmitter(() -> NOW, randomPlacementGenerator);
      dataprocAPI = mock(DataprocAPI.class);
      arguments = new SpydraArgument();
      clientId = "my-client-id";
    }

    @Test
    public void acquirePooledCluster() throws Exception {
      final Duration age = Duration.ofMinutes(30);

      ImmutableList<Cluster> clusters =
          ImmutableList.of(
              perfectCluster(clientId, 0, 1),
              perfectCluster(clientId, 1, 1));

      SpydraArgument.Pooling pooling = new SpydraArgument.Pooling();
      pooling.setLimit(2); // 2 perfectly suited clusters above!
      pooling.setMaxAge(age);
      arguments.setClientId(clientId);
      arguments.setPooling(pooling);

      when(dataprocAPI.listClusters(eq(arguments), anyMapOf(String.class, String.class)))
          .thenReturn(clusters);
      when(dataprocAPI.createCluster(arguments))
          .thenReturn(Optional.of(perfectCluster(clientId, 0, 1)));

      boolean result = poolingSubmitter.acquireCluster(arguments, dataprocAPI);
      assertTrue("Failed to acquire a cluster", result);
      verify(dataprocAPI, never()).createCluster(arguments);

      assertTrue("A healthy cluster should be kept without problems.",
          poolingSubmitter.releaseCluster(arguments, dataprocAPI));
      verify(dataprocAPI, never()).deleteCluster(arguments);

    }

    @Test
    public void avoidAncientCluster() throws Exception {
      final Duration age = Duration.ofMinutes(10);
      ImmutableList<Cluster> clusters =
          ImmutableList.of(perfectCluster(clientId, 0, 0),
              perfectCluster(clientId, 1, 0));

      SpydraArgument.Pooling pooling = new SpydraArgument.Pooling();
      pooling.setLimit(1); // 2 reasonable but old clusters above. Pool is "full", but none usable.
      pooling.setMaxAge(age);
      arguments.setClientId(clientId);
      arguments.setPooling(pooling);

      when(dataprocAPI.listClusters(eq(arguments), anyMapOf(String.class, String.class)))
          .thenReturn(clusters);
      when(dataprocAPI.createCluster(arguments))
          .thenReturn(Optional.of(perfectCluster(clientId, 0, 3)));

      boolean result = poolingSubmitter.acquireCluster(arguments, dataprocAPI);
      assertTrue("Failed to acquire a cluster", result);
      verify(dataprocAPI, times(1)).createCluster(arguments);
    }

    @Test
    public void acquireNewCluster() throws Exception {
      ImmutableList<Cluster> clusters =
          ImmutableList.of(perfectCluster(clientId, 0, 1));

      SpydraArgument.Pooling pooling = new SpydraArgument.Pooling();
      pooling.setLimit(2);
      pooling.setMaxAge(Duration.ofMinutes(30));
      arguments.setPooling(pooling);
      arguments.setClientId(clientId);

      ClusterPlacement clusterPlacement = new ClusterPlacementBuilder()
          .clusterNumber(1)
          .clusterGeneration(1)
          .build();

      reset(randomPlacementGenerator);
      when(randomPlacementGenerator.randomPlacement(anyListOf(ClusterPlacement.class)))
          .thenReturn(clusterPlacement);

      when(dataprocAPI.listClusters(eq(arguments), anyMapOf(String.class, String.class)))
          .thenReturn(clusters);
      when(dataprocAPI.createCluster(arguments))
          .thenReturn(Optional.of(perfectCluster(clientId, 1, 1)));
      boolean result = poolingSubmitter.acquireCluster(arguments, dataprocAPI);
      assertTrue("Failed to acquire a cluster", result);
      verify(dataprocAPI, times(1)).createCluster(arguments);
    }

    @Test
    public void releaseCluster() throws Exception {
      ImmutableList<Cluster> clusters =
          ImmutableList.of(perfectCluster(clientId, 0, 1),
              perfectCluster(clientId, 1, 1));

      SpydraArgument.Pooling pooling = new SpydraArgument.Pooling();
      pooling.setLimit(2);
      pooling.setMaxAge(Duration.ofMinutes(30));
      arguments.setPooling(pooling);
      arguments.setClientId(clientId);
      arguments.cluster.setName(spydraClusterName);

      when(dataprocAPI.listClusters(eq(arguments), anyMapOf(String.class, String.class)))
          .thenReturn(clusters);
      assertTrue("A healthy cluster should be kept without problems.",
          poolingSubmitter.releaseCluster(arguments, dataprocAPI));
      verify(dataprocAPI).listClusters(eq(arguments), anyMapOf(String.class, String.class));
      verify(dataprocAPI, never()).deleteCluster(arguments);
    }

    @Test
    public void releaseErrorCluster() throws Exception {
      ImmutableList<Cluster> clusters =
          ImmutableList.of(errorCluster(clientId, 0, 0),
              errorCluster(clientId, 1, 0));

      SpydraArgument.Pooling pooling = new SpydraArgument.Pooling();
      pooling.setLimit(2);
      pooling.setMaxAge(Duration.ofMinutes(30));
      arguments.setPooling(pooling);
      arguments.setClientId(clientId);
      arguments.cluster.setName(spydraClusterName);

      when(dataprocAPI.listClusters(eq(arguments), anyMapOf(String.class, String.class)))
          .thenReturn(clusters);
      when(dataprocAPI.deleteCluster(arguments)).thenReturn(true);
      assertTrue("A broken cluster should be collected without problems.",
          poolingSubmitter.releaseCluster(arguments, dataprocAPI));
      verify(dataprocAPI).listClusters(eq(arguments), anyMapOf(String.class, String.class));
      verify(dataprocAPI).deleteCluster(arguments);
    }
  }
}
