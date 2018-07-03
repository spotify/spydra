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

import org.junit.Assert;
import org.junit.Test;

public class SpydraArgumentTest {

  @Test
  public void testMergeEmptyArguments() {
    SpydraArgument first = new SpydraArgument();
    SpydraArgument second = new SpydraArgument();
    SpydraArgument merged = SpydraArgument.merge(first, second);
    Assert.assertNotNull(merged);
  }

  @Test
  public void testMergeListArguments() {
    SpydraArgument first = new SpydraArgument();
    SpydraArgument second = new SpydraArgument();

    first.getCluster().getOptions().put("metadata", "foo=bar");
    second.getCluster().getOptions().put("metadata", "foo2=bar2");

    SpydraArgument merged = SpydraArgument.merge(first, second);

    String mergedValue = merged.getCluster().getOptions().get("metadata");
    Assert.assertEquals(2, mergedValue.split(",").length);
    Assert.assertTrue(mergedValue.contains("foo=bar"));
    Assert.assertTrue(mergedValue.contains("foo2=bar2"));
  }

  @Test
  public void testAddListOption() {
    SpydraArgument args = new SpydraArgument();

    args.getCluster().getOptions().put("metadata", "foo=bar");
    SpydraArgument.addOption(args.getCluster().getOptions(), "metadata", "foo2=bar2");

    String mergedValue = args.getCluster().getOptions().get("metadata");
    Assert.assertEquals(2, mergedValue.split(",").length);
    Assert.assertTrue(mergedValue.contains("foo=bar"));
    Assert.assertTrue(mergedValue.contains("foo2=bar2"));
  }

}
