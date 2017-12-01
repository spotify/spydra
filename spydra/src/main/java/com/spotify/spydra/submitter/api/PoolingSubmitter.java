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

import com.spotify.spydra.api.DataprocAPI;
import com.spotify.spydra.api.model.Cluster;
import com.spotify.spydra.model.SpydraArgument;
import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PoolingSubmitter extends DynamicSubmitter {

  private static final Logger LOGGER = LoggerFactory.getLogger(DynamicSubmitter.class);

  public PoolingSubmitter() {
    super();
  }

  public PoolingSubmitter(String account) {
    super(account);
  }

  static final class Conditions {

    static boolean isSpydraCluster(Cluster c) {
      return c.clusterName.startsWith("spydra-");
    }

    static boolean isRunning(Cluster c) {
      return c.status.state.equals(Cluster.Status.RUNNING);
    }

    static boolean isYoung(Cluster c, SpydraArgument arguments) {
      return c.status.stateStartTime.plus(arguments.getPooling().getMaxAge())
          .isAfter(ZonedDateTime.now(ZoneOffset.UTC));
    }

    public static boolean mayCreateMoreClusters(List<Cluster> filteredClusters,
        SpydraArgument arguments) {
      boolean may = filteredClusters.size() < arguments.getPooling().getLimit();
      if (may) {
        LOGGER.info(String.format("Got room for more clusters, %d out of %d.",
            filteredClusters.size(), arguments.getPooling().getLimit()));
      } else {
        LOGGER.info(String.format("Should use a pooled cluster, have %d maximum %d clusters.",
            filteredClusters.size(), arguments.getPooling().getLimit()));
      }
      return may;
    }
  }

  @Override
  public boolean acquireCluster(SpydraArgument arguments, DataprocAPI dataprocAPI)
      throws IOException {
    try {
      Collection<Cluster> clusters = dataprocAPI.listClusters(arguments);
      LOGGER.info(
          String.format("Found %d clusters, finding suitable cluster to pool on", clusters.size()));
      List<Cluster> filteredClusters = clusters.stream()
          .filter(Conditions::isSpydraCluster)
          .filter(Conditions::isRunning)
          .filter(c -> Conditions.isYoung(c, arguments))
          // Sort and limit to eventually over time arrive at the limit
          .sorted(Comparator.comparing(c -> c.clusterName))
          .limit(arguments.getPooling().getLimit())
          .collect(Collectors.toList());
      if (Conditions.mayCreateMoreClusters(filteredClusters, arguments)) {
        return createNewCluster(arguments, dataprocAPI);
      } else {
        //TODO: TW do a weighted sample on metrics instead.
        Collections.shuffle(filteredClusters);
        Cluster cluster = filteredClusters.get(0);
        mutateForCluster(arguments, cluster.clusterName, cluster.config.gceClusterConfig.zoneUri);
        return true;
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to pool a cluster: ", e);
    }

    // All has failed us - try to create a cluster the good 'ol fashioned way.
    return super.acquireCluster(arguments, dataprocAPI);
  }

  @Override
  public boolean releaseCluster(SpydraArgument arguments, DataprocAPI dataprocAPI)
      throws IOException {
    boolean shouldCollect;
    // If we're pooling clusters the cluster will live long enough for history to be collected
    shouldCollect = !arguments.isPoolingEnabled();

    // Check if the cluster is healthy, if it's not it might not be able to self-destruct.
    shouldCollect = shouldCollect || dataprocAPI.listClusters(arguments).stream()
        .filter(cluster -> cluster.clusterName.equals(arguments.getCluster().getName()))
        .findAny().map(cluster -> cluster.status.state.equals(Cluster.Status.ERROR)).orElse(false);

    return !shouldCollect || super.releaseCluster(arguments, dataprocAPI);
  }
}
