/**
 *  Copyright 2023 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;


/**
 * Class that logs assignments one task at a time, keeping each log line under 1 MB in size
 */
public class AssignmentTaskMapLogger {
  private final Logger _log;
  public static final double SIZE_LIMIT_BYTES = 1024 * 1024;
  private static final double BUFFER_1KB =  1024;
  private static final int BUFFER_ADJUSTED_LIMIT = (int) (SIZE_LIMIT_BYTES - BUFFER_1KB);

  /**
   * Constructor
   * @param logger logger to be used for logging
   */
  public AssignmentTaskMapLogger(Logger logger) {
    _log = logger;
  }

  /**
   * logs assigned tasks for each host
   */
  public void logAssignment(Map<String, Set<DatastreamTask>> taskMap) {
    for (String host: taskMap.keySet()) {
      for (DatastreamTask task: taskMap.get(host)) {
        logTask(task.toString(), host, 1);
      }
    }
  }

  /**
   * prints one log line for each task smaller than size limit
   * @param task
   */
  private void logTask(String task, String host, int part) {
    if (isLessThanSizeLimit(task)) {
      if (part == 1) {
        _log.info("Host={}: Live task={}", host, task);
      } else {
        _log.info("Host={}: Live task (part {})={}", host, part, task);
      }
    } else {
      _log.info("Host={}: Live task (part {})={}", host, part, task.substring(0, BUFFER_ADJUSTED_LIMIT));
      logTask(task.substring(BUFFER_ADJUSTED_LIMIT), host, part + 1);
    }
  }

  /**
   * helper function to get the size of a task in string form (default charset UTF) in bytes
   * @param task string form of task to measure
   * @return size of task in bytes
   */
  private double getBlobSizeInBytes(String task) {
    if (Objects.nonNull(task) && !task.isEmpty()) {
      return (task.getBytes(StandardCharsets.UTF_8).length);
    }
    return 0;
  }

  /**
   * helper function to check if task size is less than size limit
   * @param task task to check size of
   * @return true if object is less than size limit
   */
  private boolean isLessThanSizeLimit(String task) {
    double sizeInMB = getBlobSizeInBytes(task);
    return sizeInMB + BUFFER_1KB < SIZE_LIMIT_BYTES;
  }
}
