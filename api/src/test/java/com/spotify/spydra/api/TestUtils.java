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

package com.spotify.spydra.api;

import static org.junit.Assert.assertFalse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.mockito.ArgumentCaptor;

public abstract class TestUtils {

  private TestUtils() { }

  public static void assertNoneStartsWith(List<String> command, String prefix) {
    assertFalse(command.stream().anyMatch(s -> s.startsWith(prefix)));
  }

  public static String fromResource(String resource) {
    char[] buffer = new char[1024];
    StringBuilder sb = new StringBuilder();
    int len = 0;
    try (BufferedReader r = new BufferedReader(new InputStreamReader(TestUtils.class.getResourceAsStream(resource)))) {
      while ((len = r.read(buffer)) >= 0) {
        sb.append(buffer, 0, len);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return sb.toString();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static <T> ArgumentCaptor<List<T>> listArgumentCaptor(Class<T> type) {
    return ArgumentCaptor.forClass((Class)List.class);
  }
}
