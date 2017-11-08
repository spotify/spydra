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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
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

  static Cluster perfectCluster() {
    Cluster cluster = new Cluster();
    cluster.clusterName = spydraClusterName;
    Cluster.Status status = new Cluster.Status();
    status.state = Cluster.Status.RUNNING;
    status.stateStartTime = ZonedDateTime.now(ZoneOffset.UTC);
    cluster.status = status;
    cluster.config.gceClusterConfig.metadata.heartbeat =
        Optional.of(cluster.status.stateStartTime.plusMinutes(60));
    return cluster;
  }

  static Cluster nonSpydraCluster() {
    Cluster cluster = new Cluster();
    cluster.clusterName = nonSpydraClusterName;
    Cluster.Status status = new Cluster.Status();
    status.state = Cluster.Status.RUNNING;
    status.stateStartTime = ZonedDateTime.now(ZoneOffset.UTC);
    return cluster;
  }

  static Cluster creatingCluster() {
    Cluster cluster = perfectCluster();
    cluster.status.state = Cluster.Status.CREATING;
    return cluster;
  }

  static Cluster ancientCluster() {
    Cluster cluster = perfectCluster();
    cluster.config.gceClusterConfig.metadata.heartbeat =
        Optional.of(cluster.status.stateStartTime.minusMinutes(30));
    return cluster;
  }

  static Cluster errorCluster() {
    Cluster cluster = perfectCluster();
    cluster.status.state = Cluster.Status.ERROR;
    return cluster;
  }

  public static class PoolingSubmitterTest {

    PoolingSubmitter poolingSubmitter;
    DataprocAPI dataprocAPI;
    SpydraArgument arguments;

    @Before
    public void before() {
      poolingSubmitter = new PoolingSubmitter();
      dataprocAPI = mock(DataprocAPI.class);
      arguments = new SpydraArgument();
    }

    @Test
    public void acquireAndReleasePooledCluster() throws Exception {
      ImmutableList<Cluster> clusters =
          ImmutableList.of(perfectCluster(), perfectCluster(), nonSpydraCluster());

      SpydraArgument.Pooling pooling = new SpydraArgument.Pooling();
      pooling.setLimit(2); // 2 perfectly suited clusters above!
      pooling.setMaxAge(Duration.ofMinutes(30));
      arguments.setPooling(pooling);
      arguments.setCollectorTimeoutMinutes(10);

      when(dataprocAPI.listClusters(arguments))
          .thenReturn(clusters);
      when(dataprocAPI.createCluster(arguments))
          .thenReturn(Optional.of(perfectCluster()));
      boolean result = poolingSubmitter.acquireCluster(arguments, dataprocAPI);
      assertTrue("Failed to acquire a cluster", result);
      verify(dataprocAPI, never()).createCluster(arguments);

      assertTrue("A healthy cluster should be kept without problems.", poolingSubmitter.releaseCluster(arguments, dataprocAPI));
      verify(dataprocAPI, never()).deleteCluster(arguments);

    }

    @Test
    public void acquireNewCluster() throws Exception {
      ImmutableList<Cluster> clusters =
          ImmutableList.of(perfectCluster(), perfectCluster(), nonSpydraCluster());

      SpydraArgument.Pooling pooling = new SpydraArgument.Pooling();
      pooling.setLimit(3); // 2 perfectly suited clusters above, so room for 1 more!
      pooling.setMaxAge(Duration.ofMinutes(30));
      arguments.setPooling(pooling);
      arguments.setCollectorTimeoutMinutes(10);

      when(dataprocAPI.listClusters(arguments))
          .thenReturn(clusters);
      when(dataprocAPI.createCluster(arguments))
          .thenReturn(Optional.of(perfectCluster()));
      boolean result = poolingSubmitter.acquireCluster(arguments, dataprocAPI);
      assertTrue("Failed to acquire a cluster", result);
      verify(dataprocAPI, times(1)).createCluster(arguments);
    }

    @Test
    public void releaseCluster() throws Exception {
      arguments.setPooling(new SpydraArgument.Pooling());
      arguments.cluster.setName(spydraClusterName);

      when(dataprocAPI.listClusters(arguments)).thenReturn(ImmutableList.of(perfectCluster()));
      assertTrue("A healthy cluster should be kept without problems.", poolingSubmitter.releaseCluster(arguments, dataprocAPI));
      verify(dataprocAPI).listClusters(arguments);
      verify(dataprocAPI, never()).deleteCluster(arguments);
    }

    @Test
    public void releaseErrorCluster() throws Exception {
      arguments.setPooling(new SpydraArgument.Pooling());
      arguments.cluster.setName(spydraClusterName);

      when(dataprocAPI.listClusters(arguments)).thenReturn(ImmutableList.of(errorCluster()));
      when(dataprocAPI.deleteCluster(arguments)).thenReturn(true);
      assertTrue("A broken cluster should be collected without problems.", poolingSubmitter.releaseCluster(arguments, dataprocAPI));
      verify(dataprocAPI).listClusters(arguments);
      verify(dataprocAPI).deleteCluster(arguments);
    }
  }

  public static class PoolingConditionsTest {

    @Test
    public void isSpydraCluster() throws Exception {
      assertTrue(PoolingSubmitter.Conditions.isSpydraCluster(perfectCluster()));
      assertFalse(PoolingSubmitter.Conditions.isSpydraCluster(nonSpydraCluster()));
    }

    @Test
    public void isRunning() throws Exception {
      assertTrue(PoolingSubmitter.Conditions.isRunning(perfectCluster()));
      assertFalse(PoolingSubmitter.Conditions.isRunning(creatingCluster()));
    }

    @Test
    public void isYoung() throws Exception {
      SpydraArgument arguments = new SpydraArgument();
      SpydraArgument.Pooling pooling = new SpydraArgument.Pooling();
      pooling.setMaxAge(Duration.ofMinutes(30));
      arguments.setPooling(pooling);
      assertTrue(PoolingSubmitter.Conditions.isYoung(perfectCluster(), arguments));
    }

    @Test
    public void mayCreateMoreClusters() throws Exception {
      ImmutableList<Cluster> clusters =
          ImmutableList.of(perfectCluster(), perfectCluster());
      SpydraArgument arguments = new SpydraArgument();
      SpydraArgument.Pooling pooling = new SpydraArgument.Pooling();
      pooling.setLimit(2);
      pooling.setMaxAge(Duration.ofMinutes(30));
      arguments.setPooling(pooling);

      assertFalse(PoolingSubmitter.Conditions.mayCreateMoreClusters(clusters, arguments));

      pooling.setLimit(3);
      assertTrue(PoolingSubmitter.Conditions.mayCreateMoreClusters(clusters, arguments));
    }
  }
}
