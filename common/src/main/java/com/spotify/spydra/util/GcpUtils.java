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

package com.spotify.spydra.util;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.gax.paging.Page;
import com.google.auth.Credentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.spotify.spydra.model.SpydraArgument;
import java.io.IOException;
import java.net.URI;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcpUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(GcpUtils.class);

  private static final String HADOOP_CONFIG_NAME = "spydra-default.xml";

  public static Storage storage;

  private static final GcpConfiguration gcpConfiguration = GcpConfiguration.create();

  public void configureCredentialFromEnvironment(Configuration configuration)
      throws IOException {
    configuration.set("fs.gs.project.id", getProjectId());
    Optional<String> credentialsJsonPath = getJsonCredentialsPath();
    if (credentialsJsonPath.isPresent()) {
      LOGGER.info("Found service account credentials from: " + credentialsJsonPath.get());
      configuration.setBoolean("fs.gs.auth.service.account.enable", true);
      configuration.set("fs.gs.auth.service.account.json.keyfile", credentialsJsonPath.get());
    } else {
      LOGGER.info("Using default hadoop configuration");
    }
  }

  public void configureClusterProjectFromCredential(SpydraArgument arguments)
      throws IOException {
    arguments.getCluster().getOptions()
      .put(SpydraArgument.OPTION_PROJECT, getProjectId());
  }

  public FileSystem fileSystemForUri(URI uri) throws IOException {
    Configuration configuration = new Configuration();
    configuration.addResource(HADOOP_CONFIG_NAME);
    configureCredentialFromEnvironment(configuration);
    return FileSystem.get(uri, configuration);
  }

  public void configureStorageFromEnvironment()
          throws IOException {
    storage = StorageOptions.newBuilder()
        .setCredentials(getCredentials())
        .setProjectId(getProjectId())
        .build()
        .getService();
  }

  public Bucket checkBucket(String bucketName) {
    return Objects.requireNonNull(storage.get(bucketName, Storage.BucketGetOption.fields()),
            "No such bucket");
  }

  public Bucket createBucket(String bucketName) {
    Bucket bucket = storage.create(BucketInfo.newBuilder(bucketName)
            .setLocation("europe-west1")
            .build());
    return bucket;
  }

  public Blob createBlob(String bucketName, String blobName) {
    BlobId blobId = BlobId.of(bucketName, blobName);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
    Blob blob = storage.create(blobInfo);
    return blob;
  }

  public Page<Blob> listBucket(String bucketName, String directory) {
    Bucket bucket = Objects.requireNonNull(storage.get(bucketName, Storage.BucketGetOption.fields()),
            "Please provide bucket name.");
    Page<Blob> blobs = bucket.list(Storage.BlobListOption.prefix(directory));
    return blobs;
  }

  public long getCount(String bucketName, String directory) {
    Iterable<Blob> bucketIterator = listBucket(bucketName, directory).iterateAll();
    return StreamSupport.stream(bucketIterator.spliterator(), false).count();
  }

  public String getProjectId() {
      return gcpConfiguration.getProjectId();
  }

  public Optional<String> getUserId() {
      return gcpConfiguration.getUserId();
  }

  public Optional<String> getJsonCredentialsPath() {
      return gcpConfiguration.getUserId();
  }

  public Credentials getCredentials() {
      return gcpConfiguration.getCredentials();
  }

  public GoogleCredential getCredential() {
      return gcpConfiguration.getCredential();
  }

}
