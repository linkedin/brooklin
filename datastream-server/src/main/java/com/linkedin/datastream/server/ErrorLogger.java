/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;

import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;

/**
 * Simple utility class for logging and throwing/returning a restli exception.
 * A shortened random UUID, as well as the server instance name,
 * is attached to each error message to facilitate the trouble shooting
 */
public final class ErrorLogger {
  private final Logger _logger;
  private final String _instance;

  /**
   * Constructing an ErrorLogger
   * @param logger logger to be used
   * @param instance server instance name
   */
  public ErrorLogger(Logger logger, String instance) {
    Validate.notNull(logger, "null logger");
    if (StringUtils.isBlank(instance)) {
      _instance = "UnknownInstance";
    } else {
      _instance = instance;
    }
    _logger = logger;
  }

  /**
   * Log error message and throw RestliServiceException
   * @param status HTTP status
   * @param msg error message
   */
  public void logAndThrowRestLiServiceException(HttpStatus status, String msg) {
    logAndThrowRestLiServiceException(status, msg, null);
  }

  /**
   * Log an error message and throw RestliServiceException afterwards with the specified status.
   * If an inner exception is present and:
   *   - error code is 5xx: include the full inner exception in the server-side logs.
   *   - otherwise: include only the short description of the exception in the logs.
   * @param status HTTP status
   * @param msg error message
   * @param e inner exception
   */
  public void logAndThrowRestLiServiceException(HttpStatus status, String msg, Exception e) {
    String id = UUID.randomUUID().toString();
    id = id.substring(0, Math.min(6, id.length()));
    String cause = e == null ? "None" : e.getMessage();

    if (status.getCode() >= HttpStatus.S_500_INTERNAL_SERVER_ERROR.getCode()) {
      // Logger ignores any null args so no need to validate e
      _logger.error("[{}] {}", id, msg, e);
    } else {
      _logger.warn("[{}] {}, cause={}", id, msg, cause);
    }

    throw new RestLiServiceException(status,
        String.format("msg=%s; cause=%s; instance=%s; id=%s;", msg, cause, _instance, id));
  }
}
