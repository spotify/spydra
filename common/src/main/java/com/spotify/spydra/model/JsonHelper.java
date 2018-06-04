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

package com.spotify.spydra.model;

import static com.fasterxml.jackson.databind.DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY;
import static com.fasterxml.jackson.databind.PropertyNamingStrategy.SNAKE_CASE;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import java.io.IOException;
import java.io.InputStream;

public class JsonHelper {
  protected static final ObjectMapper OBJECT_MAPPER = objectMapper();

  public static <T> T fromString(String json, Class<T> clazz) throws IOException {
    return OBJECT_MAPPER.readValue(json, clazz);
  }

  public static <T> T fromStream(InputStream in, Class<T> clazz) throws IOException {
    return OBJECT_MAPPER.readValue(in, clazz);
  }

  public static String toString(Object object) throws IOException {
    return OBJECT_MAPPER.writeValueAsString(object);
  }

  public static ObjectMapper objectMapper() {
    return new ObjectMapper()
        .setPropertyNamingStrategy(SNAKE_CASE)
        .enable(ACCEPT_SINGLE_VALUE_AS_ARRAY)
        .registerModule(new JavaTimeModule())
        .registerModule(new Jdk8Module())
        .registerModule(new ParameterNamesModule())
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }
}
