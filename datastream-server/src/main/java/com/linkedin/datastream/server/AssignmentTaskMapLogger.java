/**
 *  Copyright 2023 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;


/**
 * Class that logs assignments one task at a time, keeping each log line under 1 MB in size
 */
public class AssignmentTaskMapLogger {
  private final Map<String, Set<DatastreamTask>> _taskMap;
  private final Logger _log;
  private static final double BUFFER_1KB =  1.0 / 1024.0;
  private static final int NUM_OF_CHAR_IN_1MB = 1024 * 1024 / 2;

  /**
   * Constructor
   * @param taskMap assignment to be logged
   * @param logger logger to be used for logging
   */
  public AssignmentTaskMapLogger(Map<String, Set<DatastreamTask>> taskMap, Logger logger) {
    _taskMap = taskMap;
    _log = logger;
  }

  /**
   * logs assigned tasks for each host
   */
  public void logAssignment() {
    for (String host: _taskMap.keySet()) {
      for (DatastreamTask task: _taskMap.get(host)) {
        logTask(task.toString(), host, 1);
      }
    }
  }

  /**
   * prints one log line for each task less than 1MB in size
   * @param task
   */
  public void logTask(String task, String host, int part) {
    try {
      if (isLessThan1MB(task)) {
        if (part == 1) {
          _log.info("Host={}: Live task={}", host, part, task);
        } else {
          _log.info("Host={}: Live task (part {})={}", host, part, task);
        }
      } else {
        _log.info("Host={}: Live task (part {})={}", host, part, task.substring(0, NUM_OF_CHAR_IN_1MB));
        logTask(task.substring(NUM_OF_CHAR_IN_1MB), host, part + 1);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * helper function to serialize object for size measurement
   * @param obj object to serialize
   * @return serialized byte array of object
   * @throws Exception
   */
  public byte[] serializeObject(Object obj) throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(bos);
    out.writeObject(obj);
    out.flush();
    return bos.toByteArray();
  }

  /**
   * helper function to check if an object is less than 1 MB in size
   * @param obj object to check size of
   * @return true if object is less than 1MB in size
   * @throws Exception
   */
  public boolean isLessThan1MB(Object obj) throws Exception {
    byte[] serializedData = serializeObject(obj);
    double sizeInMB = serializedData.length / 1024.0 / 1024.0;
    return sizeInMB + BUFFER_1KB < 1.0;
  }
}
