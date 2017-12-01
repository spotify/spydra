package com.spotify.spydra.submitter.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.spotify.spydra.api.DataprocAPI;
import com.spotify.spydra.api.model.Cluster;
import com.spotify.spydra.model.SpydraArgument;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Optional;
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

  @Before
  public void before() {
    dynamicSubmitter = new DynamicSubmitter();
    dataprocAPI = mock(DataprocAPI.class);
    arguments = new SpydraArgument();
  }

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

  static Cluster errorCluster() {
    Cluster cluster = perfectCluster();
    cluster.status.state = Cluster.Status.ERROR;
    return cluster;
  }

  @Test
  public void randomizeZoneIfAbsent() throws Exception {
    String expected = "default_zone";
    arguments.defaultZones = ImmutableList.of(expected);

    dynamicSubmitter.randomizeZoneIfAbsent(arguments);
    assertEquals(expected, arguments.cluster.getOptions().get("zone"));
  }

  @Test
  public void releaseErrorCluster() throws Exception {
    arguments.cluster.setName(spydraClusterName);
    when(dataprocAPI.listClusters(arguments)).thenReturn(ImmutableList.of(errorCluster()));
    when(dataprocAPI.deleteCluster(arguments)).thenReturn(true);
    assertTrue("A broken cluster should be collected without problems.", dynamicSubmitter.releaseCluster(arguments, dataprocAPI));
    verify(dataprocAPI).deleteCluster(arguments);
  }
}
