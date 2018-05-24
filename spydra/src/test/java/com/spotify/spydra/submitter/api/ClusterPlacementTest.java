package com.spotify.spydra.submitter.api;

import org.junit.Test;

public class ClusterPlacementTest {


  @Test
  public void testComputeGeneration() {
    for(long time =99; time < 105; time++) {
      long g = ClusterPlacement.computeGeneration(0, 3, time, 10);
      long g1 = ClusterPlacement.computeGeneration(1, 3, time, 10);
      long g2 = ClusterPlacement.computeGeneration(2, 3, time, 10);
      System.out.println(String.format("%03d %03d %03d", g, g1, g2));
    }
  }
}
