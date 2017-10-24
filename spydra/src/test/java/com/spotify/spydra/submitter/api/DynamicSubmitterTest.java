package com.spotify.spydra.submitter.api;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.spotify.spydra.model.SpydraArgument;
import org.junit.Test;

/**
 * Created by TwN on 2017-04-20.
 */
public class DynamicSubmitterTest {

  @Test
  public void randomizeZoneIfAbsent() throws Exception {
    DynamicSubmitter submitter = new DynamicSubmitter();
    SpydraArgument argument = new SpydraArgument();
    String expected = "default_zone";
    argument.defaultZones = ImmutableList.of(expected);

    submitter.randomizeZoneIfAbsent(argument);
    assertEquals(expected, argument.cluster.getOptions().get("zone"));
  }

}