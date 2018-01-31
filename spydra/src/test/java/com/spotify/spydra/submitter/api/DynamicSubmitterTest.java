package com.spotify.spydra.submitter.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.spotify.spydra.api.DataprocAPI;
import com.spotify.spydra.api.model.Cluster;
import com.spotify.spydra.model.SpydraArgument;
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
            ImmutableList.of(PoolingTest.errorCluster(clientId), PoolingTest.errorCluster(clientId));

    SpydraArgument.Pooling pooling = new SpydraArgument.Pooling();
    arguments.setPooling(pooling);
    arguments.setClientId(clientId);
    arguments.cluster.setName(spydraClusterName);

    when(dataprocAPI.listClusters(eq(arguments), anyMap()))
            .thenReturn(clusters);
    when(dataprocAPI.deleteCluster(arguments)).thenReturn(true);
    assertTrue("A broken cluster should be collected without problems.", dynamicSubmitter.releaseCluster(arguments, dataprocAPI));
    verify(dataprocAPI).deleteCluster(arguments);
  }
}
