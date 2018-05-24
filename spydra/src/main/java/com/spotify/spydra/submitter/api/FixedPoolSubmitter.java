package com.spotify.spydra.submitter.api;

import static com.spotify.spydra.model.SpydraArgument.OPTIONS_FILTER_LABEL_PREFIX;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.spotify.spydra.api.DataprocAPI;
import com.spotify.spydra.api.gcloud.GcloudClusterAlreadyExistsException;
import com.spotify.spydra.api.model.Cluster;
import com.spotify.spydra.model.SpydraArgument;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The FixedPoolSubmitter pools cluster in a rotating, fixed size pool.
 * <p>
 * The FixedPoolSubmitter addresses issues with the PoolingSubmitter significantly exceeding the
 * configured limit. When running many clients simultaneously, they would all observe a free slot
 * and create a cluster. Eventually these clusters exceeding the limit would be collected.
 * For large amounts of clients, this would commonly lead to clusters in an ERROR state due to
 * exceeding quota.
 * <p>
 * In this implementation we use an algorithm to createClusterPlacement clusters in slots according to a configured
 * limit and maximum age at a certain time and placed into a random slot. The algorithm relies on
 * the assumption that multiple clients creating a cluster with the same name fails for all but 1
 * client. The failing clients will wait for a while and then select a cluster from the list of
 * clusters available.
 *
 * The placement_token of a cluster is based two-part:<ul>
 * <li>slot_number: a random number between 0 and Limit</li>
 * <li>- time_derivative: {@code (Time - slot_number * (Age // Limit) ) // Age}</li>
 * </ul> together these form a unique identifier for each clusters' lifetime.
 * For each slot, it's cluster lifetime is offset by Age // Limit. This has as affect that
 * the limit is exceeded by one cluster at a time which should become idle and thus collected before
 * the next horde of clients come in. The algorithm is implemented in {@link ClusterPlacement#createClusterPlacement}.
 * Example scenario's have been worked out in
 * <a href="https://docs.google.com/spreadsheets/d/1Sfw_yxNSYJjHYtiHGQQf1ZHnNTqc7nGyXGGts_EaupM">a spreadsheet.</a>
 */
public class FixedPoolSubmitter extends DynamicSubmitter {

  @VisibleForTesting
  static final String FIXED_POOLED_CLUSTER_CLIENTID_LABEL =
      "spydra-fixed-pooling-cluster-client-id";
  public static final String SPYDRA_PLACEMENT_TOKEN_LABEL = "spydra-placement-token";
  public static final String SPYDRA_UNPLACED_TOKEN = "unplaced";
  private Supplier<Long> timeSource;

  public FixedPoolSubmitter(Supplier<Long> timeSource) {
    super();
    this.timeSource = timeSource;
  }

  @Override
  public boolean acquireCluster(SpydraArgument arguments, DataprocAPI dataprocAPI)
      throws IOException {

    List<Cluster> existingPoolableClusters =
        dataprocAPI.listClusters(arguments, poolableClusterFilter(arguments.getClientId()));

    List<ClusterPlacement> allPlacements =
        ClusterPlacement.all(timeSource, arguments.getPooling());

    List<Cluster> existingPlacementClusters =
        ClusterPlacement.filterClusters(existingPoolableClusters, allPlacements);

    Collections.shuffle(allPlacements);
    ClusterPlacement randomPlacement = allPlacements.get(0);

    Cluster cluster = randomPlacement.findIn(existingPlacementClusters)
        .orElse(createNewCluster(arguments, dataprocAPI, randomPlacement));

    setTargetCluster(arguments, cluster.clusterName,
        cluster.config.gceClusterConfig.zoneUri);

    return true;
  }

  private Cluster createNewCluster(SpydraArgument arguments, DataprocAPI dataprocAPI,
                                   ClusterPlacement placement)
      throws IOException {
    // Label the pooled cluster with the client id. Unknown client ids all end up in their own pool.
    arguments.addOption(arguments.cluster.options, SpydraArgument.OPTION_LABELS,
        FIXED_POOLED_CLUSTER_CLIENTID_LABEL + "=" + arguments.getClientId());

    arguments.addOption(arguments.cluster.options, SpydraArgument.OPTION_LABELS,
        SPYDRA_PLACEMENT_TOKEN_LABEL + "=" + placement.token());

    String clusterName = generateName(arguments.getClientId(), placement.token());
    try {
      return super.createNewCluster(arguments, dataprocAPI, () -> clusterName)
          .orElseThrow(() -> new IOException("Failed to create cluster: " + clusterName));
    } catch (GcloudClusterAlreadyExistsException e) {

      List<Cluster> existingClusters =
          dataprocAPI.listClusters(arguments, Collections.singletonMap("clusterName", clusterName));

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
  public boolean releaseCluster(SpydraArgument arguments, DataprocAPI dataprocAPI)
      throws IOException {

    Map<String, String> clusterFilter = ImmutableMap.of(
        "status.state", "ERROR",
        "clusterName", arguments.getCluster().getName()
    );

    boolean shouldRelease = dataprocAPI.listClusters(arguments, clusterFilter).stream()
        .findAny().map(cluster -> cluster.status.state.equals(Cluster.Status.ERROR)).orElse(false);

    return !shouldRelease || super.releaseCluster(arguments, dataprocAPI);
  }

  private static Map<String, String> poolableClusterFilter(String clientId) {
    return ImmutableMap.of(
        OPTIONS_FILTER_LABEL_PREFIX + SPYDRA_CLUSTER_LABEL, "",
        OPTIONS_FILTER_LABEL_PREFIX + FIXED_POOLED_CLUSTER_CLIENTID_LABEL, clientId
    );
  }
}
