package com.spotify.spydra.submitter.api;

import java.util.List;

public interface RandomPlacementGenerator {

  ClusterPlacement randomPlacement(List<ClusterPlacement> placements);
}
