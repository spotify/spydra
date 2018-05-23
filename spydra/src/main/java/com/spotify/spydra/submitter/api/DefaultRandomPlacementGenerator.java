package com.spotify.spydra.submitter.api;

import java.util.List;
import java.util.Random;

public class DefaultRandomPlacementGenerator implements RandomPlacementGenerator {

  @Override
  public ClusterPlacement randomPlacement(final List<ClusterPlacement> placements) {
    return placements.get(new Random().nextInt(placements.size()));
  }
}
