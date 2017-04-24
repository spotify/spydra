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

package com.spotify.spydra.metrics.impl;

import com.spotify.spydra.metrics.Metrics;
import com.spotify.spydra.model.SpydraArgument;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingMetrics extends Metrics {
  private static final Logger LOGGER = LoggerFactory.getLogger(LoggingMetrics.class);

  public LoggingMetrics(String user) {
    super(user);
  }

  @Override
  public void clusterCreation(SpydraArgument arguments, boolean success) {
    LOGGER.info("Cluster was created with success=" + success);
  }

  @Override
  public void clusterDeletion(SpydraArgument arguments, boolean success) {
    LOGGER.info("Cluster was deleted with success=" + success);
  }

  @Override
  public void jobSubmission(SpydraArgument arguments, String type, boolean success) {
    LOGGER.info(type + " job was submitted with success=" + success);
  }

  @Override
  public void metadataUpdate(SpydraArgument arguments, String key, boolean success) {
    LOGGER.info("Metadata with key " + key + "was updated with success=" + success);
  }

  @Override
  public void fatalError(SpydraArgument argument, Throwable throwable) {
    LOGGER.info("Fatal error was caught" + throwable.getCause());
  }

  @Override
  public void executionResult(SpydraArgument argument, boolean success) {
    LOGGER.info("Execution finished with success=" + success);
  }

  @Override
  public void flush() {
    // Nothing to do
  }
}
