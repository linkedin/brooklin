/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class that contains the helper utility methods for File system operations.
 */
public class FileUtils {

  private static final Random RANDOM = new Random();

  private static final Logger LOG = LoggerFactory.getLogger(FileUtils.class.getName());

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
      String errorMessage = "could not create temp directory: " + file.getAbsolutePath();
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, null);
    }

    file.deleteOnExit();
    return file;
  }

  /**
   * Constructs a directory in the temp folder
   * This directory will NOT get deleted when the process exits.
   * @param dirName
   *   Prefix to be used for the directory that needs to be created.
   * @return
   *   Object referencing the directory that is created.
   */
  public static File constructDirectoryInTempDir(String dirName) {
    File file = new File(System.getProperty("java.io.tmpdir"), dirName);
    if (!file.mkdirs()) {
      String errorMessage = "could not create temp directory: " + file.getAbsolutePath();
      LOG.info(errorMessage);
    }

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
