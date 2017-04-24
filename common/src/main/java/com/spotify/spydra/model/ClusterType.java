/*
 * Copyright 2017 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.spydra.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Represents the different cluster types support by the submitter.
 */
public enum ClusterType {
  ON_PREMISE,
  DATAPROC,
  NULL;

  static final String ON_PREMISE_NAME = "onpremise";
  static final String DATAPROC_NAME = "dataproc";
  static final String NULL_NAME = "null";

  // This method is used by jackson to deserialize from the json representation
  @JsonCreator
  public static ClusterType forValue(String value) {
    switch (value) {
      case ON_PREMISE_NAME:
        return ClusterType.ON_PREMISE;
      case DATAPROC_NAME:
        return ClusterType.DATAPROC;
      case NULL_NAME:
        return ClusterType.NULL;
      default:
        throw new IllegalArgumentException("No such cluster type");
    }
  }

  @JsonValue
  public String toValue() {
    switch (this) {
      case ON_PREMISE:
        return ON_PREMISE_NAME;
      case DATAPROC:
        return DATAPROC_NAME;
      case NULL:
        return NULL_NAME;
      default:
        throw new RuntimeException("Can not serialize unknown cluster type: " + this);
    }
  }
}
