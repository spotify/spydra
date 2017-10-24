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

import com.google.common.base.Throwables;
import com.spotify.spydra.metrics.impl.LoggingMetrics;
import com.spotify.spydra.model.SpydraArgument;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsFactory.class);

  private static Metrics metrics;

  public static synchronized Metrics getInstance() {
    if (metrics == null) {
      LOGGER.info("MetricsFactory was not initialized. Using default implementation.");
      metrics = new LoggingMetrics("");
    }
    return metrics;
  }

  public static synchronized void initialize(SpydraArgument arguments, String user) {
    if (metrics != null) {
      throw new RuntimeException("Initializing MetricsFactory multiple times");
    }
    try {
      Class clazz =
          Class.forName(arguments.getMetricClass(), true, MetricsFactory.class.getClassLoader());
      Constructor<Metrics> constructor = clazz.getConstructor(String.class);
      metrics = constructor.newInstance(user);
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException
        | NoSuchMethodException | InvocationTargetException e) {
      Throwables.propagate(e);
    }
  }
}
