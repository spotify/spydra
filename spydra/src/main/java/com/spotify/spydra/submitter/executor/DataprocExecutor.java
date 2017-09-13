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

package com.spotify.spydra.submitter.executor;

import static com.spotify.spydra.model.SpydraArgument.OPTION_SERVICE_ACCOUNT;

import com.spotify.spydra.api.DataprocAPI;
import com.spotify.spydra.model.SpydraArgument;

import java.io.IOException;

public class DataprocExecutor implements Executor {

  @Override
  public boolean submit(SpydraArgument arguments) throws IOException {
    DataprocAPI dataprocAPI =
        new DataprocAPI(arguments.getCluster().getOptions().get(OPTION_SERVICE_ACCOUNT));
    dataprocAPI.dryRun(arguments.isDryRun());
    return dataprocAPI.submit(arguments);
  }
}
