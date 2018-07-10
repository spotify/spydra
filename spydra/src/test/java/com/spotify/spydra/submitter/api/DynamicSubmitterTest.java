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
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.spotify.spydra.api.DataprocApi;
import com.spotify.spydra.api.model.Cluster;
import com.spotify.spydra.model.SpydraArgument;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by TwN on 2017-04-20.
 */
public class DynamicSubmitterTest {

  private static final String spydraClusterName = "spydra-uuid";

  DynamicSubmitter dynamicSubmitter;
  DataprocApi dataprocApi;
  SpydraArgument arguments;
  String clientId;

  @Before
  public void before() {
    dynamicSubmitter = new DynamicSubmitter();
    dataprocApi = mock(DataprocApi.class);
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
}
