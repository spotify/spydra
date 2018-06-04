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

package com.spotify.spydra;

import static org.junit.Assert.assertTrue;

import com.google.common.base.Joiner;
import com.spotify.spydra.submitter.runner.CliParser;
import java.util.List;

public class CliTestHelpers {
  public static <T> void ensureAllThrow(CliParser<T> parser, List<String[]> argsList, Class exceptionCls) {
    for (String[] args : argsList) {
      String errorMsg = "Should throw for: " + Joiner.on(";").join(args);
      assertTrue(errorMsg, checkThrows(parser, args, exceptionCls));
    }
  }

  public static <T> boolean checkThrows(CliParser<T> parser, String[] args, Class exceptionCls) {
    boolean threw = false;

    try {
      parser.parse(args);
    } catch (Exception e) {
      if (exceptionCls.isInstance(e)) {
        threw = true;
      }
    }
    return threw;
  }

  public static String toStrOpt(String optionName, String optionValue) {
    return String.format("--%s=%s", optionName, optionValue);
  }
}
