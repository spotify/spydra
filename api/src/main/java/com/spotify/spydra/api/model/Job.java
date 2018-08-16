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

@JsonIgnoreProperties(ignoreUnknown = true)
public class Job {

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Status {
    public static final String STATE_UNSPECIFIED = "STATE_UNSPECIFIED";
    public static final String PENDING = "PENDING";
    public static final String SETUP_DONE = "SETUP_DONE";
    public static final String RUNNING = "RUNNING";
    public static final String CANCEL_PENDING = "CANCEL_PENDING";
    public static final String CANCEL_STARTED = "CANCEL_STARTED";
    public static final String CANCELLED = "CANCELLED";
    public static final String DONE = "DONE";
    public static final String ERROR = "ERROR";
    public static final String ATTEMPT_FAILURE = "ATTEMPT_FAILURE";
    public String state;

    public boolean isInProggress() {
      return state.equals(PENDING) || state.equals(RUNNING) || state.equals(SETUP_DONE);
    }

    public boolean isDone() {
      return state.equals(DONE);
    }

    public boolean isFailed() {
      boolean isNotFailed = isInProggress() || isDone();
      return !isNotFailed;
    }

  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Reference {
    public String jobId;
  }

  public Reference reference;
  public Status status;

}
