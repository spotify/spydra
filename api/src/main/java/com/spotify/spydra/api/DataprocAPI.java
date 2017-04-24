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

package com.spotify.spydra.api;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import com.spotify.spydra.api.gcloud.GcloudExecutor;
import com.spotify.spydra.api.model.Cluster;
import com.spotify.spydra.metrics.Metrics;
import com.spotify.spydra.metrics.MetricsFactory;
import com.spotify.spydra.model.SpydraArgument;

import java.io.IOException;
import java.util.Collection;

public class DataprocAPI {
  private final Metrics metrics;
  private final GcloudExecutor gcloud;

  public DataprocAPI() {
    gcloud = new GcloudExecutor();
    metrics = MetricsFactory.getInstance();
  }

  @VisibleForTesting
  DataprocAPI(GcloudExecutor gcloud, Metrics metrics) {
    this.gcloud = gcloud;
    this.metrics = metrics;
  }

  public void dryRun(boolean dryRun) {
    gcloud.dryRun(dryRun);
  }

  public boolean createCluster(SpydraArgument arguments) throws IOException {
    boolean success = false;
    try {
      success = gcloud.createCluster(arguments.getCluster().getName(),
          arguments.getCluster().getOptions());
    } finally {
      metrics.clusterCreation(arguments, success);
    }
    return success;
  }

  public boolean deleteCluster(SpydraArgument arguments) throws IOException {
    ImmutableMap<String, String> args = ImmutableMap.of(SpydraArgument.OPTION_PROJECT,
        arguments.getCluster().getOptions().get(SpydraArgument.OPTION_PROJECT));
    boolean success = false;
    try {
      success = gcloud.deleteCluster(arguments.getCluster().getName(), args);
    } finally {
      metrics.clusterDeletion(arguments, success);
    }
    return success;
  }

  public boolean submit(SpydraArgument arguments) throws IOException {
    boolean success = false;
    try {
      success = gcloud.submit(arguments.getJobType(), arguments.getSubmit().getOptions(),
          arguments.getSubmit().getJobArgs());
    } finally {
      metrics.jobSubmission(arguments, "dataproc", success);
    }
    return success;
  }

  public boolean updateMasterMetadata(SpydraArgument arguments, String key, String value)
      throws IOException {
    ImmutableMap<String, String> args = ImmutableMap.of(
        SpydraArgument.OPTION_PROJECT, arguments.getCluster().getOptions().get(SpydraArgument.OPTION_PROJECT),
        SpydraArgument.OPTION_ZONE, arguments.getCluster().getOptions().get(SpydraArgument.OPTION_ZONE));
    String project = arguments.getCluster().getOptions().get(SpydraArgument.OPTION_PROJECT);
    String masterNode = gcloud.getMasterNode(project, arguments.getCluster().getName());

    boolean success = false;
    try {
      success = gcloud.updateMetadata(masterNode, args, key, value);
    } finally {
      metrics.metadataUpdate(arguments, key, success);
    }
    return success;
  }

  public Collection<Cluster> listClusters(SpydraArgument arguments) throws IOException {
    return gcloud.listClusters(arguments.cluster.getOptions().get("project"));
  }
}
