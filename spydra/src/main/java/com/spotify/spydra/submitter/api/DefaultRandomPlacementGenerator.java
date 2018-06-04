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

import java.util.List;
import java.util.Random;

public class DefaultRandomPlacementGenerator implements RandomPlacementGenerator {

  @Override
  public ClusterPlacement randomPlacement(final List<ClusterPlacement> placements) {
    return placements.get(new Random().nextInt(placements.size()));
  }
}
