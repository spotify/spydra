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

import com.jayway.jsonpath.JsonPath;
import com.spotify.spydra.model.SpydraArgument;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;

public class GcpUtils {
  public static final String HADOOP_CONFIG_NAME = "spydra-default.xml";

  public void configureCredentialFromEnvironment(Configuration configuration)
      throws IOException {
    String json = credentialJsonFromEnv();
    configuration.setBoolean("fs.gs.auth.service.account.enable", true);
    configuration.set("fs.gs.auth.service.account.json.keyfile", jsonCredentialPath());
    configuration.set("fs.gs.project.id", projectFromJsonCredential(json));
  }

  public String jsonCredentialPath() {
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

  public String projectFromJsonCredential(String json) {
    return JsonPath.read(json, "$.project_id");
  }

  public String userIdFromJsonCredential(String json) {
    return JsonPath.read(json, "$.client_email");
  }

  public void configureClusterProjectFromCredential(SpydraArgument arguments)
      throws IOException {
    String json = credentialJsonFromEnv();
    arguments.getCluster().getOptions()
        .put(SpydraArgument.OPTION_PROJECT, projectFromJsonCredential(json));
  }

  public FileSystem fileSystemForUri(URI uri) throws IOException {
    Configuration configuration = new Configuration();
    configuration.addResource(HADOOP_CONFIG_NAME);
    configureCredentialFromEnvironment(configuration);
    return FileSystem.get(uri, configuration);
  }
}
