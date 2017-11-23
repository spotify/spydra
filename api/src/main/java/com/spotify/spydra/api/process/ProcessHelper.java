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

package com.spotify.spydra.api.process;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessHelper.class);

  public static int executeCommand(List<String> command) throws IOException {
    LOGGER.debug("Executing command: " + StringUtils.join(command, " "));
    ProcessBuilder pb = new ProcessBuilder(command)
        .redirectError(ProcessBuilder.Redirect.INHERIT)
        .redirectOutput(ProcessBuilder.Redirect.INHERIT);

    Process p = pb.start();
    try {
      p.waitFor();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      p.destroy();
    }

    return p.exitValue();
  }

  public static boolean executeForOutput(List<String> command, StringBuilder outputBuilder)
      throws IOException {
    LOGGER.debug("Executing command: " + StringUtils.join(command, " "));
    ProcessBuilder pb = new ProcessBuilder(command)
        .redirectError(ProcessBuilder.Redirect.INHERIT)
        .redirectOutput(ProcessBuilder.Redirect.PIPE);

    Process p = pb.start();
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
      String line;
      while ((line = in.readLine()) != null) {
        outputBuilder.append(line + System.getProperty("line.separator"));
      }
      int exitCode = p.waitFor();
      return exitCode == 0;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      p.destroy();
      throw new IOException("Failed to read process output", e);
    } catch (IOException e) {
      p.destroy();
      throw e;
    }
  }
}
