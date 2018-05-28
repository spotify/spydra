package com.spotify.spydra.submitter.api;


import static com.spotify.spydra.submitter.api.PoolingSubmitter.SPYDRA_PLACEMENT_TOKEN_LABEL;
import static com.spotify.spydra.submitter.api.PoolingSubmitter.SPYDRA_UNPLACED_TOKEN;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import com.spotify.spydra.api.model.Cluster;
import com.spotify.spydra.model.SpydraArgument;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.immutables.value.Value;

@Value.Style(
    visibility = Value.Style.ImplementationVisibility.PRIVATE,
    builderVisibility = Value.Style.BuilderVisibility.PACKAGE,
    overshadowImplementation = true)
@Value.Immutable
abstract class ClusterPlacement {

  abstract int clusterNumber();

  abstract long clusterGeneration();

  String token() {
    return String.format("%s-%s", clusterNumber(), clusterGeneration());
  }

  static ClusterPlacement from(String token) {
    String[] splits = token.split("-");
    if (splits.length != 2) {
      throw new IllegalArgumentException("Could not parse token: " + token);
    }
    try {
      return new ClusterPlacementBuilder()
          .clusterNumber(Integer.valueOf(splits[0]))
          .clusterGeneration(Long.valueOf(splits[1]))
          .build();
    } catch (NumberFormatException nfe) {
      throw new IllegalArgumentException("Could not parse token: " + token, nfe);
    }
  }

  static List<ClusterPlacement> all(Supplier<Long> timeSource, SpydraArgument.Pooling pooling) {
    return IntStream.range(0, pooling.getLimit())
        .mapToObj(clusterNumber -> createClusterPlacement(timeSource, clusterNumber, pooling))
        .collect(toList());
  }

  static ClusterPlacement createClusterPlacement(Supplier<Long> timeSource,
                                                 int clusterNumber,
                                                 SpydraArgument.Pooling pooling) {
    int limit = pooling.getLimit();
    long time = timeSource.get() / 1000;
    long age = pooling.getMaxAge().getSeconds();

    long generation = computeGeneration(clusterNumber, limit, time, age);

    return new ClusterPlacementBuilder()
        .clusterNumber(clusterNumber)
        .clusterGeneration(generation)
        .build();
  }

  static long computeGeneration(int clusterNumber, int limit, long time, long age) {
    return (long) ((time - clusterNumber * (age / (float) limit)) / age);
  }

  private static ClusterPlacement placement(Cluster cluster) {
    return from(cluster.labels.getOrDefault(SPYDRA_PLACEMENT_TOKEN_LABEL, SPYDRA_UNPLACED_TOKEN));
  }

  static List<Cluster> filterClusters(List<Cluster> clusters,
                                      List<ClusterPlacement> allPlacements) {
    Set<String> clusterPlacements = allPlacements.stream()
        .map(ClusterPlacement::token)
        .collect(toSet());

    return clusters.stream()
        .filter(cluster -> clusterPlacements.contains(placement(cluster).token()))
        .collect(toList());
  }

  public Optional<Cluster> findIn(List<Cluster> clusters) {
    return clusters.stream()
        .filter(cluster -> this.equals(placement(cluster)))
        .findFirst();
  }

}
