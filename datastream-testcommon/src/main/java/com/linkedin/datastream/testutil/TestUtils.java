/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


/**
 * Helper class for writing tests.
 */
public final class TestUtils {

  private TestUtils() {
  }

  /**
   * Generate a list of random strings
   * @param count number of random strings to generate
   * @return List of Strings
   */
  public static List<String> generateStrings(int count) {
    List<String> generatedValues = new ArrayList<>();
    for (int index = 0; index < count; index++) {
      generatedValues.add("Value_" + UUID.randomUUID().toString() + "_" + index);
    }
    return generatedValues;
  }
}
