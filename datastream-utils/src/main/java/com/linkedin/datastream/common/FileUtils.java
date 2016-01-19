package com.linkedin.datastream.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Random;


/**
 * Class that contains the helper utility methods for File system operations.
 */
public class FileUtils {

  private static final Random RANDOM = new Random();

  /**
   * Constructs a random directory with the prefix in the temp folder
   * This directory is a temporary directory and will get deleted when the process exits.
   * @param dirPrefix
   *   Prefix to be used for the directory that needs to be created.
   * @return
   *   Object referencing the directory that is created.
   */
  public static File constructRandomDirectoryInTempDir(String dirPrefix) {
    File file = new File(System.getProperty("java.io.tmpdir"), dirPrefix + RANDOM.nextInt(10000000));
    if (!file.mkdirs()) {
      throw new RuntimeException("could not create temp directory: " + file.getAbsolutePath());
    }
    file.deleteOnExit();
    return file;
  }

  /**
   * Delete the folder and all its contents recursively.
   * @param path
   *  Path that needs to be deleted.
   * @return
   *  True if the file was deleted successfully,
   *  False if not.
   * @throws FileNotFoundException
   */
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
}
