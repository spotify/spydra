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

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.spotify.spydra.model.SpydraArgument;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
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

  public void configureCredentialFromEnvironment(Configuration configuration)
      throws IOException {
    String json = credentialJsonFromEnv();
    Optional<String> projectId = projectFromJsonCredential(json);
    if (projectId.isPresent()) {
      LOGGER.info("Found service account credentials from: " + jsonCredentialPath());
      configuration.setBoolean("fs.gs.auth.service.account.enable", true);
      configuration.set("fs.gs.auth.service.account.json.keyfile", jsonCredentialPath());
      configuration.set("fs.gs.project.id", projectId.get());
    } else {
      LOGGER.info("Given credentials seem not to be of service account in path: "
                  + jsonCredentialPath());
    }
  }

  private String jsonCredentialPath() {
    return System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
  }

  public String credentialJsonFromEnv() throws IOException {
    String jsonFile = jsonCredentialPath();
    if (jsonFile == null || !Files.exists(Paths.get(jsonFile))) {
      throw new IllegalArgumentException(
          "GOOGLE_APPLICATION_CREDENTIALS needs to be set and point to a valid credential json");
    }
    return new String(Files.readAllBytes(Paths.get(jsonFile)));
  }

  public Optional<String> projectFromJsonCredential(String json) throws PathNotFoundException {
    try {
      return Optional.of(JsonPath.read(json, "$.project_id"));
    } catch (PathNotFoundException ex) {
      LOGGER.info("Could not parse project_id from credentials.");
      return Optional.empty();
    }
  }

  private Optional<String> userIdFromJsonCredential(String json) {
    try {
      return Optional.of(JsonPath.read(json, "$.client_email"));
    } catch (PathNotFoundException ex) {
      LOGGER.info("Could not parse client_email from credentials.");
      return Optional.empty();
    }
  }

  public String userIdFromJsonCredentialInEnv() throws IOException {
    return userIdFromJsonCredential(credentialJsonFromEnv()).orElseThrow(
        () -> new IllegalArgumentException(
          "No valid credentials (service account) were available to forward to the cluster."));
  }

  public void configureClusterProjectFromCredential(SpydraArgument arguments)
      throws IOException {
    String json = credentialJsonFromEnv();
    projectFromJsonCredential(json)
        .ifPresent(s -> arguments.getCluster().getOptions().put(SpydraArgument.OPTION_PROJECT, s));
  }

  public FileSystem fileSystemForUri(URI uri) throws IOException {
    Configuration configuration = new Configuration();
    configuration.addResource(HADOOP_CONFIG_NAME);
    configureCredentialFromEnvironment(configuration);
    return FileSystem.get(uri, configuration);
  }

  public void configureStorageFromEnvironment()
          throws IOException {
    String json = credentialJsonFromEnv();
    Optional<String> projectId = projectFromJsonCredential(json);
    if (projectId.isPresent()) {
      storage = StorageOptions.newBuilder()
            .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(jsonCredentialPath())))
            .setProjectId(projectId.get())
            .build()
            .getService();
    } else {
      LOGGER.info("Unable to initiate storage. Check the credentials in path: "
              + jsonCredentialPath());
    }
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
    Iterable bucketIterator = listBucket(bucketName, directory).iterateAll();
    return StreamSupport.stream(bucketIterator.spliterator(), false).count();
  }
}
