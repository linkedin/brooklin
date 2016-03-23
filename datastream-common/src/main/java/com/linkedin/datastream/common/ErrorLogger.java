package com.linkedin.datastream.common;

import org.slf4j.Logger;


/**
 * Helper utility methods for error logging.
 */
public class ErrorLogger {

  /**
   * Log error and throw DatastreamRuntimeException with inner exception
   * @param log logger object
   * @param msg error message
   * @param t inner exception
   */
  public static void logAndThrowDatastreamRuntimeException(Logger log, String msg, Throwable t) {
    if (t != null) {
      log.error(msg, t);
      throw new DatastreamRuntimeException(msg, t);
    } else {
      log.error(msg);
      throw new DatastreamRuntimeException(msg);
    }
  }
}
