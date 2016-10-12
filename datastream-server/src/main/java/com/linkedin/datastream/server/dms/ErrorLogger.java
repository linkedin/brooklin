package com.linkedin.datastream.server.dms;

import java.util.UUID;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;

import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.RestLiServiceException;

/**
 * Simple utility class for logging and throwing/returning a restli exception.
 * A random UUID is attached for each instance for more readable log output.
 */
final class ErrorLogger {
  private Logger _logger;

  public ErrorLogger(Logger logger) {
    Validate.notNull(logger, "null logger");
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
    if (e != null) {
      _logger.error(String.format("[%s] %s", id, msg), e);
      throw new RestLiServiceException(status, msg + " cause=" + e.getMessage());
    } else {
      _logger.error(String.format("[%s] %s", id, msg));
      throw new RestLiServiceException(status, msg);
    }
  }

  /**
   * Log error and return a CreateResponse
   * @param status HTTP status
   * @param msg error message
   * @return CreateResponse object
   */
  public CreateResponse logAndGetResponse(HttpStatus status, String msg) {
    return logAndGetResponse(status, msg, null);
  }

  /**
   * Log error and return a CreateResponse with inner exception
   * @param status HTTP status
   * @param msg error message
   * @param e inner exception
   * @return CreateResponse object
   */
  public CreateResponse logAndGetResponse(HttpStatus status, String msg, Exception e) {
    String id = UUID.randomUUID().toString();
    if (e != null) {
      _logger.error(String.format("[%s] %s", id, msg), e);
      return new CreateResponse(new RestLiServiceException(status, msg + " cause=" + e.getMessage()));
    } else {
      _logger.error(String.format("[%s] %s", id, msg));
      return new CreateResponse(new RestLiServiceException(status, msg));
    }
  }
}
