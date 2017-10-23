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

import static com.spotify.spydra.model.SpydraArgument.OPTION_CLASS;
import static com.spotify.spydra.model.SpydraArgument.OPTION_JAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.spotify.spydra.model.SpydraArgument;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

public class OnPremiseExecutorTest {

  @Test
  public void testGetCommand() {
    String jarPath = "jarPath";
    List<String> args = new ArrayList<>();
    args.add("arg1");
    String mainClass = "mainClass";
    String[] properties = new String[]{
        "propertyName0=propertyValue0",
        "propertyName1=propertyValue1"};

    SpydraArgument arguments = new SpydraArgument();
    arguments.getSubmit().getOptions().put(OPTION_JAR, jarPath);
    arguments.getSubmit().getOptions().put(OPTION_CLASS, mainClass);
    arguments.getSubmit().getOptions().put(SpydraArgument.OPTION_PROPERTIES,
        StringUtils.join(properties, ","));
    arguments.getSubmit().setJobArgs(args);

    OnPremiseExecutor executor = new OnPremiseExecutor();
    List<String> command = executor.getCommand(arguments);

    String[] baseCommand = StringUtils.split(OnPremiseExecutor.BASE_COMMAND);
    for (int i = 0; i < baseCommand.length; i++) {
      assertEquals(baseCommand[i], command.get(i));
    }

    assertEquals(command.get(baseCommand.length), jarPath);
    assertEquals(command.get(baseCommand.length + 1), mainClass);

    int count = 0;
    for (int i = 0; i < command.size() - 1; i++) {
      if (command.get(i).equals(OnPremiseExecutor.PROPERTY)) {
        assertTrue(command.contains(properties[0])
            || command.contains(properties[1]));
        count++;
      }
    }
    assertEquals(properties.length, count);

    // Check order of arguments (HADOOP-1056)
    String expectedCommand = "hadoop jar jarPath mainClass arg1 "
        + "-D propertyName0=propertyValue0 "
        + "-D propertyName1=propertyValue1";
    String actualCommand = StringUtils.join(command, StringUtils.SPACE);
    assertEquals(expectedCommand, actualCommand);
  }
}
