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

  public static List<String> generateStrings(int count) {
    List<String> generatedValues = new ArrayList<>();
    for (int index = 0; index < count; index++) {
      generatedValues.add("Value_" + UUID.randomUUID().toString() + "_" + index);
    }
    return generatedValues;
  }
}
