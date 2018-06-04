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

package com.spotify.spydra.api.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Cluster {

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Status {

    public static final String ERROR = "ERROR";
    public static final String CREATING = "CREATING";
    public static final String RUNNING = "RUNNING";
    public String state;
    public ZonedDateTime stateStartTime;
  }

  public Status status;

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Config {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class GceClusterConfig {

      @JsonIgnoreProperties(ignoreUnknown = true)
      public static class Metadata {

        // This is the initial heartbeat of the cluster. It is not updated.
        // Look at the MasterConfig.instanceNames[0] for updated heartbeats
        public Optional<ZonedDateTime> heartbeat = Optional.empty();
      }

      public Metadata metadata = new Metadata();
      public String zoneUri; // Even though this is an Uri-string, --zone takes it just fine
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MasterConfig {
      public List<String> instanceNames = Collections.emptyList();
    }

    public MasterConfig masterConfig = new MasterConfig();
    public GceClusterConfig gceClusterConfig = new GceClusterConfig();
  }

  public Config config = new Config();

  public String clusterName;

  public Map<String, String> labels;

  //TODO: TW Look at the `metrics` for the cluster
  //TODO: TW look at labels to see if it is a spydra created cluster?
}
