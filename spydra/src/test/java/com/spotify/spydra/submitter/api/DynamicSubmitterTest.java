package com.spotify.spydra.submitter.api;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.spydra.api.DataprocAPI;
import com.spotify.spydra.api.model.Cluster;
import com.spotify.spydra.model.SpydraArgument;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by TwN on 2017-04-20.
 */
public class DynamicSubmitterTest {

  private static final String spydraClusterName = "spydra-uuid";

  DynamicSubmitter dynamicSubmitter;
  DataprocAPI dataprocAPI;
  SpydraArgument arguments;
  String clientId;

  @Before
  public void before() {
    dynamicSubmitter = new DynamicSubmitter();
    dataprocAPI = mock(DataprocAPI.class);
    arguments = new SpydraArgument();
    clientId = "my-client-id";
  }


  @Test
  public void releaseErrorCluster() throws Exception {
    ImmutableList<Cluster> clusters =
            ImmutableList.of(errorCluster(clientId), errorCluster(clientId));

    SpydraArgument.Pooling pooling = new SpydraArgument.Pooling();
    arguments.setPooling(pooling);
    arguments.setClientId(clientId);
    arguments.cluster.setName(spydraClusterName);

    when(dataprocAPI.listClusters(eq(arguments), anyMapOf(String.class, String.class)))
            .thenReturn(clusters);
    when(dataprocAPI.deleteCluster(arguments)).thenReturn(true);
    assertTrue("A broken cluster should be collected without problems.",
        dynamicSubmitter.releaseCluster(arguments, dataprocAPI));
    verify(dataprocAPI).deleteCluster(arguments);
  }

  private static Cluster perfectCluster(final String clientid) {
    Cluster cluster = new Cluster();
    cluster.clusterName = spydraClusterName;
    Cluster.Status status = new Cluster.Status();
    status.state = Cluster.Status.RUNNING;
    status.stateStartTime = ZonedDateTime.now(ZoneOffset.UTC);
    cluster.status = status;
    cluster.labels = ImmutableMap.of(DynamicSubmitter.SPYDRA_CLUSTER_LABEL, "1");
    return cluster;
  }

  private static Cluster errorCluster(final String clientid) {
    Cluster cluster = perfectCluster(clientid);
    cluster.status.state = Cluster.Status.ERROR;
    return cluster;
  }
}
