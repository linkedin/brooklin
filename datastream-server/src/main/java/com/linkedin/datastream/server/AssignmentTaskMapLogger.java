/**
 *  Copyright 2023 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import com.linkedin.datastream.common.zk.ZkClient;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;


/**
 * Class that logs assignments one task at a time, keeping each log line under 1 MB in size
 */
public class AssignmentTaskMapLogger {
  private final Logger _log;
  private static final double ONE_MEBIBYTE = 1024 * 1024;
  private static final double ONE_MB = 1.0;
  private static final double BUFFER_1KB =  1.0 / 1024.0;
  private static final int BUFFER_ADJUSTED_1MB = 1024 * 1020;

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
   * prints one log line for each task less than 1MB in size
   * @param task
   */
  private void logTask(String task, String host, int part) {
    if (isLessThan1MB(task)) {
      if (part == 1) {
        _log.info("Host={}: Live task={}", host, task);
      } else {
        _log.info("Host={}: Live task (part {})={}", host, part, task);
      }
    } else {
      _log.info("Host={}: Live task (part {})={}", host, part, task.substring(0, BUFFER_ADJUSTED_1MB));
      logTask(task.substring(BUFFER_ADJUSTED_1MB), host, part + 1);
    }
  }

  /**
   * helper function to get the size of a task in string form (default charset UTF) in MBs
   * @param task string form of task to measure
   * @return size of task in MBS
   */
  private double getBlobSizeInMBs(String task) {
    if (Objects.nonNull(task) && !task.isEmpty()) {
      return ((double) task.getBytes(ZkClient.ZK_STRING_SERIALIZER_CHARSET).length) / ONE_MEBIBYTE;
    }
    return 0;
  }

  /**
   * helper function to check if an object is less than 1 MB in size
   * @param task task to check size of
   * @return true if object is less than 1MB in size
   */
  private boolean isLessThan1MB(String task) {
    double sizeInMB = getBlobSizeInMBs(task);
    return sizeInMB + BUFFER_1KB < ONE_MB;
  }
}
