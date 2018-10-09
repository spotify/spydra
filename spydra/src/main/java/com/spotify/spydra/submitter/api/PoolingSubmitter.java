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

import static com.spotify.spydra.model.SpydraArgument.OPTIONS_FILTER_LABEL_PREFIX;

import com.spotify.spydra.api.DataprocApi;
import com.spotify.spydra.api.gcloud.GcloudClusterAlreadyExistsException;
import com.spotify.spydra.api.model.Cluster;
import com.spotify.spydra.model.SpydraArgument;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The PoolingSubmitter pools cluster in a rotating, fixed size pool.
 *
 * <p>
 * The PoolingSubmitter addresses issues with the PoolingSubmitter significantly exceeding the
 * configured limit. When running many clients simultaneously, they would all observe a free slot
 * and create a cluster. Eventually these clusters exceeding the limit would be collected.
 * For large amounts of clients, this would commonly lead to clusters in an ERROR state due to
 * exceeding quota.
 * </p>
 *
 * <p>
 * In this implementation we use an algorithm to createClusterPlacement clusters in slots according
 * to a configured limit and maximum age at a certain time and placed into a random slot. The
 * algorithm relies on the assumption that multiple clients creating a cluster with the same name
 * fails for all but 1 client. The failing clients will wait for a while and then select a cluster
 * from the list of clusters available.
 * </p>
 *
 * <p>
 * The placement_token of a cluster is based two-part:
 * </p>
 * <ul>
 * <li>slot_number: a random number between 0 and Limit</li>
 * <li>- time_derivative: {@code (Time - slot_number * (Age // Limit) ) // Age}</li>
 * </ul>
 * <p>Together these form a unique identifier for each clusters' lifetime.
 * For each slot, it's cluster lifetime is offset by Age // Limit. This has as affect that
 * the limit is exceeded by one cluster at a time which should become idle and thus collected before
 * the next horde of clients come in. The algorithm is implemented in
 * {@link ClusterPlacement#createClusterPlacement}.
 * </p>
 */
public class PoolingSubmitter extends DynamicSubmitter {

  public static final String POOLED_CLUSTER_CLIENTID_LABEL =
      "spydra-fixed-pooling-cluster-client-id";
  public static final String SPYDRA_PLACEMENT_TOKEN_LABEL = "spydra-placement-token";
  public static final String SPYDRA_UNPLACED_TOKEN = "unplaced";

  private Supplier<Long> timeSource;
  private final RandomPlacementGenerator randomPlacementGenerator;

  public PoolingSubmitter(
      Supplier<Long> timeSource,
      RandomPlacementGenerator randomPlacementGenerator) {
    super();
    this.timeSource = timeSource;
    this.randomPlacementGenerator = randomPlacementGenerator;
  }

  @Override
  public boolean acquireCluster(SpydraArgument arguments, DataprocApi dataprocApi)
      throws IOException {

    List<Cluster> existingPoolableClusters =
        dataprocApi.listClusters(arguments, poolableClusterFilter(arguments.getClientId()));

    List<ClusterPlacement> allPlacements =
        ClusterPlacement.all(timeSource, arguments.getPooling());

    List<Cluster> existingPlacementClusters =
        ClusterPlacement.filterClusters(existingPoolableClusters, allPlacements);

    ClusterPlacement randomPlacement = randomPlacementGenerator.randomPlacement(allPlacements);
    Cluster cluster = randomPlacement.findIn(existingPlacementClusters)
        .orElseGet(() -> {
          try {
            return createNewCluster(arguments, dataprocApi, randomPlacement);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });

    setTargetCluster(arguments, cluster.clusterName, cluster.config.gceClusterConfig.zoneUri);

    return true;
  }

  private Cluster createNewCluster(
      SpydraArgument arguments,
      DataprocApi dataprocApi,
      ClusterPlacement placement
  ) throws IOException {
    // Label the pooled cluster with the client id. Unknown client ids all end up in their own pool.
    SpydraArgument.addOption(arguments.cluster.options, SpydraArgument.OPTION_CLUSTER_LABELS,
                             POOLED_CLUSTER_CLIENTID_LABEL + "=" + arguments.getClientId());

    SpydraArgument.addOption(arguments.cluster.options, SpydraArgument.OPTION_CLUSTER_LABELS,
                             SPYDRA_PLACEMENT_TOKEN_LABEL + "=" + placement.token());

    String clusterName = generateName(arguments.getClientId(), placement.token());
    try {
      return super.createNewCluster(arguments, dataprocApi, () -> clusterName)
          .orElseThrow(() -> new IOException("Failed to create cluster: " + clusterName));
    } catch (GcloudClusterAlreadyExistsException e) {

      List<Cluster> existingClusters =
          dataprocApi.listClusters(arguments, Collections.singletonMap("clusterName", clusterName));

      if (existingClusters.size() != 1) {
        throw new IllegalStateException(
            "Expected a single cluster to exists. Cluster name:" + clusterName);
      }

      return existingClusters.get(0);
    }
  }

  public static String generateName(String clientId, String placementToken) {
    return String.format("spydra-%s-%s", clientId, placementToken);
  }

  @Override
  public boolean releaseCluster(SpydraArgument arguments, DataprocApi dataprocApi)
      throws IOException {

    Map<String, String> clusterFilter = new HashMap<>();
    clusterFilter.put("status.state", "ERROR");
    clusterFilter.put("clusterName", arguments.getCluster().getName());

    boolean shouldRelease = dataprocApi.listClusters(arguments, clusterFilter).stream()
        .findAny().map(cluster -> cluster.status.state.equals(Cluster.Status.ERROR)).orElse(false);

    return !shouldRelease || super.releaseCluster(arguments, dataprocApi);
  }

  private static Map<String, String> poolableClusterFilter(String clientId) {
    Map<String, String> result = new HashMap<>();
    result.put(OPTIONS_FILTER_LABEL_PREFIX + SPYDRA_CLUSTER_LABEL, "");
    result.put(OPTIONS_FILTER_LABEL_PREFIX + POOLED_CLUSTER_CLIENTID_LABEL, clientId);
    return result;
  }
}
