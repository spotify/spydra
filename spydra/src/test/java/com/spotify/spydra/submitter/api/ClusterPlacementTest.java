package com.spotify.spydra.submitter.api;

import static com.spotify.spydra.submitter.api.PoolingSubmitter.SPYDRA_PLACEMENT_TOKEN_LABEL;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import com.spotify.spydra.api.model.Cluster;
import com.spotify.spydra.model.SpydraArgument;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class ClusterPlacementTest {

  @Test
  public void testComputeGeneration() {
    assertEquals(ClusterPlacement.computeGeneration(0, 3, 0, 30), 0);
    assertEquals(ClusterPlacement.computeGeneration(0, 3, 29, 30), 0);
    assertEquals(ClusterPlacement.computeGeneration(0, 3, 30, 30), 1);

    assertEquals(ClusterPlacement.computeGeneration(1, 3, 10, 30), 0);
    assertEquals(ClusterPlacement.computeGeneration(1, 3, 39, 30), 0);
    assertEquals(ClusterPlacement.computeGeneration(1, 3, 40, 30), 1);

    assertEquals(ClusterPlacement.computeGeneration(2, 3, 20, 30), 0);
    assertEquals(ClusterPlacement.computeGeneration(2, 3, 49, 30), 0);
    assertEquals(ClusterPlacement.computeGeneration(2, 3, 50, 30), 1);
  }

  @Test
  public void testAllPlacements() {
    final SpydraArgument.Pooling pooling = new SpydraArgument.Pooling();
    pooling.setLimit(3);
    pooling.setMaxAge(Duration.ofSeconds(30));

    final List<ClusterPlacement> all = ClusterPlacement.all(() -> 40000L, pooling);

    final List<ClusterPlacement> expectedPlacements =
        Arrays.asList(new ClusterPlacementBuilder().clusterNumber(0).clusterGeneration(1).build(),
            new ClusterPlacementBuilder().clusterNumber(0).clusterGeneration(1).build(),
            new ClusterPlacementBuilder().clusterNumber(0).clusterGeneration(0).build());

    assertThat(all, hasSize(3));
    assertThat(all, hasItems(new ClusterPlacementBuilder().clusterNumber(0).clusterGeneration(1).build(),
        new ClusterPlacementBuilder().clusterNumber(1).clusterGeneration(1).build(),
        new ClusterPlacementBuilder().clusterNumber(2).clusterGeneration(0).build()));
  }

  @Test
  public void testFrom() {
    ClusterPlacement clusterPlacement = ClusterPlacement.from("10-15");
    assertThat(clusterPlacement.clusterNumber(), is(10));
    assertThat(clusterPlacement.clusterGeneration(), is(15L));
  }

  @Test
  public void testToken() {
    final ClusterPlacement clusterPlacement =
        new ClusterPlacementBuilder().clusterNumber(10).clusterGeneration(15L).build();
    assertThat(clusterPlacement.token(), is("10-15"));
  }

  @Test
  public void testFilterClusters() {

    final List<ClusterPlacement> clusterPlacements =
        Arrays.asList(new ClusterPlacementBuilder().clusterNumber(0).clusterGeneration(1).build(),
            new ClusterPlacementBuilder().clusterNumber(1).clusterGeneration(1).build(),
            new ClusterPlacementBuilder().clusterNumber(2).clusterGeneration(0).build());

    final Cluster cluster1 = new Cluster();
    cluster1.labels = Collections.singletonMap(SPYDRA_PLACEMENT_TOKEN_LABEL, "0-1");
    final Cluster cluster2 = new Cluster();
    cluster2.labels = Collections.singletonMap(SPYDRA_PLACEMENT_TOKEN_LABEL, "10-15");

    final List<Cluster> filterClusters =
        ClusterPlacement.filterClusters(Arrays.asList(cluster1, cluster2), clusterPlacements);

    assertThat(filterClusters, hasSize(1));
    assertThat(filterClusters, contains(cluster1));
  }

  @Test
  public void testFindIn() {
    final ClusterPlacement missingPlacement =
        new ClusterPlacementBuilder().clusterNumber(0).clusterGeneration(0).build();
    final ClusterPlacement existingPlacement =
        new ClusterPlacementBuilder().clusterNumber(0).clusterGeneration(1).build();

    final Cluster cluster1 = new Cluster();
    cluster1.labels = Collections.singletonMap(SPYDRA_PLACEMENT_TOKEN_LABEL, "0-1");
    final Cluster cluster2 = new Cluster();
    cluster2.labels = Collections.singletonMap(SPYDRA_PLACEMENT_TOKEN_LABEL, "10-15");
    final List<Cluster> clusters = Arrays.asList(cluster1, cluster2);

    assertFalse(missingPlacement.findIn(clusters).isPresent());
    assertTrue(existingPlacement.findIn(clusters).isPresent());
    assertThat(existingPlacement.findIn(clusters).get(), is(cluster1));
  }
}
