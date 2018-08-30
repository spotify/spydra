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

package com.spotify.spydra.submitter.executor;

import com.spotify.spydra.api.DataprocApi;
import com.spotify.spydra.model.SpydraArgument;

import java.util.function.Supplier;

/**
 * Factory responsible for creating instances of different executors based a
 * cluster descriptor, to submit jobs for executor.
 */
public class ExecutorFactory {

  private final Supplier<DataprocApi> dataprocApiSupplier;

  public ExecutorFactory() {
    this(DataprocApi::new);
  }

  public ExecutorFactory(Supplier<DataprocApi> dataprocApiSupplier) {
    this.dataprocApiSupplier = dataprocApiSupplier;
  }

  public Executor getExecutor(SpydraArgument arguments) {
    switch (arguments.getClusterType()) {
      case DATAPROC:
        return new DataprocExecutor(dataprocApiSupplier.get());
      case ON_PREMISE:
        return new OnPremiseExecutor();
      case NULL:
        return new NullExecutor();
      default:
        throw new IllegalArgumentException("Could not instantiate executor for cluster type: "
            + arguments.getClusterType());
    }
  }
}
