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

import com.spotify.spydra.metrics.Metrics;
import com.spotify.spydra.metrics.MetricsFactory;
import com.spotify.spydra.model.SpydraArgument;
import com.spotify.spydra.submitter.executor.Executor;
import com.spotify.spydra.submitter.executor.ExecutorFactory;
import com.spotify.spydra.util.SpydraArgumentUtil;
import java.io.IOException;
import java.time.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Submitter {

  private static final Logger LOGGER = LoggerFactory.getLogger(Submitter.class);

  private final Metrics metrics = MetricsFactory.getInstance();

  private static final Clock clock = Clock.systemUTC();

  public static Submitter getSubmitter(SpydraArgument arguments) {
    SpydraArgumentUtil.checkRequiredArguments(arguments, SpydraArgumentUtil
            .isOnPremiseInvocation(arguments),
        SpydraArgumentUtil.isStaticInvocation(arguments));

    Submitter submitter; // TODO: TW These if's are getting out of hand. Make it prettier
    if (SpydraArgumentUtil.isStaticInvocation(arguments) || SpydraArgumentUtil
        .isOnPremiseInvocation(arguments)) {
      submitter = new Submitter();
    } else {
      if (arguments.isPoolingEnabled()) {
        submitter = new PoolingSubmitter(clock::millis, new DefaultRandomPlacementGenerator());
      } else {
        submitter = new DynamicSubmitter();
      }
    }
    return submitter;
  }

  public boolean executeJob(SpydraArgument arguments) {
    return executeJob(new ExecutorFactory(), arguments);
  }

  protected boolean executeJob(ExecutorFactory executorFactory, SpydraArgument arguments) {
    try {
      Executor executor = executorFactory.getExecutor(arguments);
      return executor.submit(arguments);
    } catch (IOException e) {
      LOGGER.error("Failed to submit job", e);
      metrics.fatalError(arguments, e);
      return false;
    }
  }
}
