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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataproc.Dataproc;
import com.google.api.services.dataproc.model.Cluster;
import com.google.api.services.dataproc.model.ListClustersResponse;
import com.google.auth.oauth2.GceHelper;
import com.google.common.collect.Lists;
import com.spotify.spydra.model.SpydraArgument;
import com.spotify.spydra.submitter.api.Submitter;
import com.spotify.spydra.util.GcpUtils;
import com.spotify.spydra.util.SpydraArgumentUtil;
import java.io.IOException;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.UUID;
import org.apache.hadoop.examples.WordCount;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LifecycleIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(LifecycleIT.class);

  private final static String CLIENT_ID = UUID.randomUUID().toString();

  private final static GcpUtils gcpUtils = new GcpUtils();

  @Test
  public void testLifecycle() throws Exception {
    // The way that the haddop credentials provider is buggy as it tries to contact the metadata
    // service in gcp if we use the default account
    Assume.assumeTrue("Skipping lifecycle test, not running on gce and "
                      + "GOOGLE_APPLICATION_CREDENTIALS not set",
        hasApplicationJsonOrRunningOnGce());
    SpydraArgument testArgs = SpydraArgumentUtil.loadArguments("integration-test-config.json");
    SpydraArgument arguments = SpydraArgumentUtil
        .dataprocConfiguration(CLIENT_ID, testArgs.getLogBucket(), testArgs.getRegion());
    arguments.getCluster().numWorkers(3);
    arguments.getSubmit().jar(getExamplesJarPath());
    arguments.getSubmit().setJobArgs(Lists.newArrayList("pi", "1", "1"));

    // TODO We should test the init action as well but the uploading before running the test is tricky
    // We could upload it manually to a test bucket here and set the right things
    arguments.getCluster().getOptions().remove(SpydraArgument.OPTION_INIT_ACTIONS);

    // Merge to get all other custom test arguments
    arguments = SpydraArgument.merge(arguments, testArgs);

    LOGGER.info("Using following service account to run gcloud commands locally: " +
        arguments.getCluster().getOptions().get(SpydraArgument.OPTION_ACCOUNT));
    Submitter submitter = Submitter.getSubmitter(arguments);
    assertTrue("job wasn't successful", submitter.executeJob(arguments));

    assertTrue(isClusterCollected(arguments));

    URI doneUri = URI.create(arguments.clusterProperties().getProperty(
        "mapred:mapreduce.jobhistory.done-dir"));
    LOGGER.info("Checking that we have two files in: " + doneUri);
    assertEquals(2, getFileCount(doneUri));
    URI intermediateUri = URI.create(arguments.clusterProperties().getProperty(
        "mapred:mapreduce.jobhistory.intermediate-done-dir"));
    LOGGER.info("Checking that we do not have any files in: " + intermediateUri);
    assertEquals(0, getFileCount(intermediateUri));
  }

  private boolean hasApplicationJsonOrRunningOnGce() {
    return new GcpUtils().getJsonCredentialsPath().isPresent()
           || GceHelper.runningOnComputeEngine();
  }

  private boolean isClusterCollected(SpydraArgument arguments)
      throws IOException, GeneralSecurityException {
    GoogleCredential credential = new GcpUtils().getCredential();
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
