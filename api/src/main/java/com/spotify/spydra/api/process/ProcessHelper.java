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

package com.spotify.spydra.api.process;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessHelper.class);

  public static int executeCommand(List<String> command) throws IOException {
    LOGGER.debug("Executing command: " + String.join(" ", command));
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
    LOGGER.debug("Executing command: " + String.join(" ", command));
    ProcessBuilder pb = new ProcessBuilder(command)
        .redirectError(ProcessBuilder.Redirect.PIPE)
        .redirectOutput(ProcessBuilder.Redirect.PIPE);

    Process p = pb.start();
    String lineSeparator = System.getProperty("line.separator");
    try {
      // Read from both stdout and stderr so that we don't
      // fill up buffers and deadlock the process.
      BufferedReader outReader = new BufferedReader(new InputStreamReader(p.getInputStream()));
      BufferedReader errReader = new BufferedReader(new InputStreamReader(p.getErrorStream()));

      StringBuilder output = new StringBuilder();
      StringBuilder error = new StringBuilder();

      while (p.isAlive() || outReader.ready() || errReader.ready()) {
        while (outReader.ready()) {
          output.append(outReader.readLine());
          output.append(lineSeparator);
        }

        while (errReader.ready()) {
          error.append(errReader.readLine());
          error.append(lineSeparator);
        }

        Thread.sleep(100);
      }

      int exitCode = p.waitFor();
      boolean success = exitCode == 0;

      if (success) {
        outputBuilder.append(output);
      } else {
        outputBuilder.append(error);
      }

      return success;
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
