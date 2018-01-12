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

package com.spotify.spydra.metrics;

import com.spotify.spydra.model.SpydraArgument;

public abstract class Metrics {
  private final String user;

  public Metrics(String user) {
    this.user = user;
  }

  public String getUser() {
    return user;
  }

  /**
   * Emit cluster creation metric.
   *
   * @param arguments The Spydra arguments.
   * @param zoneUri   The zone URI where the cluster was created after successful cluster creation.
   *                  This can be null, in case the cluster creation failed.
   * @param success   Whether the cluster creation was successful.
   */
  public abstract void clusterCreation(SpydraArgument arguments, String zoneUri, boolean success);

  public abstract void clusterDeletion(SpydraArgument arguments, boolean success);

  public abstract void jobSubmission(SpydraArgument arguments, String type, boolean success);

  public abstract void metadataUpdate(SpydraArgument arguments, String key, boolean success);

  public abstract void metadataRemoval(SpydraArgument arguments, String key, boolean success);

  public abstract void fatalError(SpydraArgument argument, Throwable throwable);

  public abstract void executionResult(SpydraArgument argument, boolean success);

  public abstract void flush();
}
