package com.linkedin.datastream.testutil;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
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

  /**
   * Append lines to an existing file. This is needed because there is no append support of
   * FileUtils.writeLines from commons-io 1.4, the version Datastream indirectly references.
   * @param file File to be appended
   * @param lines lines to be appended to the file
   * @throws IOException
   */
  public static void appendLines(File file, List<String> lines) throws IOException {
    try (FileOutputStream out = new FileOutputStream(file, /* append */ true)) {
      IOUtils.writeLines(lines, null, out, null);
    }
  }
}
