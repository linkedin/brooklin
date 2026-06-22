/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;

import static com.linkedin.datastream.common.DatastreamMetadataConstants.CREATE_VALIDATION_TIME_MS;


/**
 * Unit tests for {@link Coordinator} that run standalone, without an embedded cluster. The full
 * integration-style test lives in {@code TestCoordinator} in the datastream-server-restli module.
 */
@Test
public class TestCoordinatorStandalone {

  private static Datastream datastreamWithValidationTime(String createValidationTimeMs) {
    Datastream ds = new Datastream();
    StringMap metadata = new StringMap();
    if (createValidationTimeMs != null) {
      metadata.put(CREATE_VALIDATION_TIME_MS, createValidationTimeMs);
    }
    ds.setMetadata(metadata);
    return ds;
  }

  @Test
  public void testGetCreateValidationTimeMsPresent() {
    // Present and positive -> included as-is
    Assert.assertEquals(Coordinator.getCreateValidationTimeMs(datastreamWithValidationTime("1500")), 1500L);
  }

  @Test
  public void testGetCreateValidationTimeMsAbsent() {
    // Property not set -> excluded (0)
    Assert.assertEquals(Coordinator.getCreateValidationTimeMs(datastreamWithValidationTime(null)), 0L);
  }

  @Test
  public void testGetCreateValidationTimeMsNonPositive() {
    // Zero or negative -> excluded (0)
    Assert.assertEquals(Coordinator.getCreateValidationTimeMs(datastreamWithValidationTime("0")), 0L);
    Assert.assertEquals(Coordinator.getCreateValidationTimeMs(datastreamWithValidationTime("-5")), 0L);
  }

  @Test
  public void testGetCreateValidationTimeMsMalformed() {
    // Not a valid long -> excluded (0)
    Assert.assertEquals(Coordinator.getCreateValidationTimeMs(datastreamWithValidationTime("not-a-number")), 0L);
  }
}
