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
   * Log error and throw RestliServiceException
   * @param status HTTP status
   * @param msg error message
   */
  public void logAndThrowRestLiServiceException(HttpStatus status, String msg) {
    logAndThrowRestLiServiceException(status, msg, null);
  }

  /**
   * Log error and throw RestliServiceException with inner exception
   * @param status HTTP status
   * @param msg error message
   * @param e inner exception
   */
  public void logAndThrowRestLiServiceException(HttpStatus status, String msg, Exception e) {
    String id = UUID.randomUUID().toString();
    id = id.substring(0, Math.min(6, id.length()));
    if (e != null) {
      _logger.error(String.format("[%s] %s", id, msg), e);
      throw new RestLiServiceException(status,
          String.format("msg=%s; cause=%s; instance=%s; id=%s;", msg, e.toString(), _instance, id));
    } else {
      _logger.error(String.format("[%s] %s", id, msg));
      throw new RestLiServiceException(status,
          String.format("msg=%s; cause=%s; instance=%s; id=%s;", msg, "None", _instance, id));
    }
  }
}
