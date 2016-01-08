package com.linkedin.datastream.testutil;

import java.io.File;
import java.io.FileNotFoundException;
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
  private static final Random RANDOM = new Random();

  private TestUtils() {
  }

  public static File constructTempDir(String dirPrefix) {
    File file = new File(System.getProperty("java.io.tmpdir"), dirPrefix + RANDOM.nextInt(10000000));
    if (!file.mkdirs()) {
      throw new RuntimeException("could not create temp directory: " + file.getAbsolutePath());
    }
    file.deleteOnExit();
    return file;
  }

  public synchronized static int getAvailablePort() {
    try {
      ServerSocket socket = new ServerSocket(0);
      try {
        return socket.getLocalPort();
      } finally {
        socket.close();
      }
    } catch (IOException e) {
      throw new IllegalStateException("Cannot find available port: " + e.getMessage(), e);
    }
  }

  public static boolean deleteFile(File path) throws FileNotFoundException {
    if (!path.exists()) {
      throw new FileNotFoundException(path.getAbsolutePath());
    }
    boolean ret = true;
    if (path.isDirectory()) {
      for (File f : path.listFiles()) {
        ret = ret && deleteFile(f);
      }
    }
    return ret && path.delete();
  }

  public static List<String> generateStrings(int count) {
    List<String> generatedValues = new ArrayList<>();
    for (int index = 0; index < count; index++) {
      generatedValues.add("Value_" + UUID.randomUUID().toString() + "_" + index);
    }
    return generatedValues;
  }
}
