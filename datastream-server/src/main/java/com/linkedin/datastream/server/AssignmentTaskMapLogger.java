/**
 *  Copyright 2023 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;

import static com.linkedin.datastream.common.LogUtils.logStringsUnderSizeLimit;


/**
 * Class that logs assignments one task at a time, keeping each log line under a size limit
 */
public class AssignmentTaskMapLogger {
  private final Logger _log;
  private double _sizeLimit;

  /**
   * Constructor
   * @param logger logger to be used for logging
   * @param logSizeLimitInBytes size limit for each log line in bytes
   */
  public AssignmentTaskMapLogger(Logger logger, double logSizeLimitInBytes) {
    _log = logger;
    _sizeLimit = logSizeLimitInBytes;
  }

  /**
   * logs assigned tasks for each host
   */
  public void logAssignment(Map<String, Set<DatastreamTask>> taskMap) {
    for (String host: taskMap.keySet()) {
      for (DatastreamTask task: taskMap.get(host)) {
        String logContext = String.format("Host=%s: Live task", host);
        logStringsUnderSizeLimit(_log, task.toString(), logContext, _sizeLimit);
      }
    }
  }
}
