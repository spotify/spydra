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

package com.spotify.spydra;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataproc.Dataproc;
import com.google.api.services.dataproc.model.Cluster;
import com.google.api.services.dataproc.model.ListClustersResponse;
import com.google.common.collect.Lists;

import com.spotify.spydra.model.ClusterType;
import com.spotify.spydra.model.SpydraArgument;
import com.spotify.spydra.submitter.api.DynamicSubmitter;
import com.spotify.spydra.submitter.api.Submitter;
import com.spotify.spydra.util.GcpUtils;
import com.spotify.spydra.util.SpydraArgumentUtil;

import org.apache.hadoop.examples.WordCount;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LifecycleIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(LifecycleIT.class);

  private final static int INTERVAL = 30 * 1000;
  private final static String CLIENT_ID = UUID.randomUUID().toString();

  private final static GcpUtils gcpUtils = new GcpUtils();

  @Test
  public void testLifecycle() throws Exception {
    SpydraArgument arguments = SpydraArgumentUtil.loadArguments("integration-test-config.json");
    gcpUtils.configureClusterProjectFromCredential(arguments);
    arguments.setClusterType(ClusterType.DATAPROC);
    arguments.getCluster().getOptions().put("num-workers", "3");
    arguments.getSubmit().getOptions().put(SpydraArgument.OPTION_JAR, getExamplesJarPath());
    arguments.getSubmit().setJobArgs(Lists.newArrayList("pi", "1", "1"));
    arguments.setHeartbeatIntervalSeconds(INTERVAL);
    arguments.setClientId(CLIENT_ID);
    String json = gcpUtils.credentialJsonFromEnv();
    String userId = gcpUtils.userIdFromJsonCredential(json);
    arguments = SpydraArgumentUtil.mergeConfigurations(arguments, userId);
    arguments.replacePlaceholders();

    SpydraArgumentUtil.checkRequiredArguments(arguments, false, false);

    // TODO We should test the init action as well but the uploading before running the test is tricky
    // We could upload it manually to a test bucket here and set the right things
    arguments.getCluster().getOptions().remove(SpydraArgument.OPTION_INIT_ACTIONS);

    Submitter submitter = new DynamicSubmitter();
    assertTrue("job wasn't successful", submitter.executeJob(arguments));

    assertTrue(isClusterCollected(arguments));

    URI doneUri = URI.create(arguments.clusterProperties().getProperty(
        "mapred:mapreduce.jobhistory.done-dir"));
    assertEquals(2, getFileCount(doneUri));
    URI intermediateUri = URI.create(arguments.clusterProperties().getProperty(
        "mapred:mapreduce.jobhistory.intermediate-done-dir"));
    assertEquals(0, getFileCount(intermediateUri));
  }

  private boolean isClusterCollected(SpydraArgument arguments)
      throws IOException, GeneralSecurityException {
    GoogleCredential credential = GoogleCredential.fromStream(
        new ByteArrayInputStream(gcpUtils.credentialJsonFromEnv().getBytes()));
    if (credential.createScopedRequired()) {
      credential =
          credential.createScoped(
              Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
    }

    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
    Dataproc dataprocService =
        new Dataproc.Builder(httpTransport, jsonFactory, credential)
            .setApplicationName("Google Cloud Platform Sample")
            .build();

    Dataproc.Projects.Regions.Clusters.List request =
        dataprocService.projects().regions().clusters().list(
            arguments.getCluster().getOptions().get(SpydraArgument.OPTION_PROJECT),
            arguments.getRegion());
    ListClustersResponse response;
    do {
      response = request.execute();
      if (response.getClusters() == null) continue;

      String clusterName = arguments.getCluster().getName();
      for (Cluster cluster : response.getClusters()) {
        if (cluster.getClusterName().equals(clusterName)) {
          String status = cluster.getStatus().getState();
          LOGGER.info("Cluster state is" + status);
          return status.equals("DELETING");
        }
      }

      request.setPageToken(response.getNextPageToken());
    } while (response.getNextPageToken() != null);
    return true;
  }

  private int getFileCount(URI uri) throws IOException {
    FileSystem fs = gcpUtils.fileSystemForUri(uri);
    RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path(uri), true);
    int count = 0;
    while (it.hasNext()) {
      it.next();
      count++;
    }
    return count;
  }

  private String getExamplesJarPath() {
    Class clazz = WordCount.class;
    return clazz.getProtectionDomain().getCodeSource().getLocation().getPath();
  }
}
