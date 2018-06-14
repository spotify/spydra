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
import com.spotify.spydra.model.SpydraArgument.Pooling;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class ClusterPlacementTest {

  @Test
  public void testClustereneration() {
    // Random offset is 20 and 6 for clusterNumber 0 and 1 and age 30
    final Pooling pooling = new Pooling();
    pooling.setLimit(2);
    pooling.setMaxAge(Duration.ofSeconds(30));
    assertEquals(ClusterPlacement.createClusterPlacement(() -> 0L, 0, pooling)
            .clusterGeneration(),0);
    assertEquals(ClusterPlacement.createClusterPlacement(() -> 39000L, 0, pooling)
        .clusterGeneration(), 1);
    assertEquals(ClusterPlacement.createClusterPlacement(() -> 40000L, 0, pooling)
        .clusterGeneration(), 2);

    assertEquals(ClusterPlacement.createClusterPlacement(() -> 0L, 1, pooling)
        .clusterGeneration(), 0);
    assertEquals(ClusterPlacement.createClusterPlacement(() -> 23000L, 1, pooling)
        .clusterGeneration(), 0);
    assertEquals(ClusterPlacement.createClusterPlacement(() -> 24000L, 1, pooling)
        .clusterGeneration(), 1);
  }

  @Test
  public void testAllPlacements() {
    final Pooling pooling = new Pooling();
    pooling.setLimit(3);
    pooling.setMaxAge(Duration.ofSeconds(30));

    // Random offsets are 20, 6 and 18 for the clusterNumbers 0, 1 and 2 and age 30
    final List<ClusterPlacement> all = ClusterPlacement.all(() -> 40000L, pooling);

    assertThat(all, hasSize(3));
    assertThat(all, hasItems(
        new ClusterPlacementBuilder().clusterNumber(0).clusterGeneration(2).build(),
        new ClusterPlacementBuilder().clusterNumber(1).clusterGeneration(1).build(),
        new ClusterPlacementBuilder().clusterNumber(2).clusterGeneration(1).build()));
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
