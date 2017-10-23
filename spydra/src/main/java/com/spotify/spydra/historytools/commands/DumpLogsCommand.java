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

package com.spotify.spydra.historytools.commands;

import com.google.auto.value.AutoValue;
import org.apache.hadoop.yarn.api.records.ApplicationId;

@AutoValue
public abstract class DumpLogsCommand {

  DumpLogsCommand() {
  }

  public abstract String clientId();

  public abstract ApplicationId applicationId();

  public abstract String username();

  public abstract String logBucket();

  public static Builder builder() {
    return new AutoValue_DumpLogsCommand.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder clientId(String clientId);

    public abstract Builder applicationId(ApplicationId jobId);

    public abstract Builder username(String username);

    public abstract Builder logBucket(String logBucket);

    public abstract DumpLogsCommand build();
  }
}
