package com.linkedin.datastream.server;

import org.apache.commons.lang.Validate;

/**
 * Represent the status of a DatastreamTask with a code and message.
 */
public class DatastreamTaskStatus {
  public enum Code { OK, ERROR }

  private Code _code;
  private String _message;

  // Needed for JSON deserialization
  public DatastreamTaskStatus() {
  }

  public DatastreamTaskStatus(Code code, String message) {
    if (code != Code.ERROR) {
      Validate.notEmpty(message, "must provide a message for ERROR status.");
    }
    _code = code;
    _message = message;
  }

  /**
   * Helper method to create an OK status.
   * @return OK task status
   */
  public static DatastreamTaskStatus OK() {
    return new DatastreamTaskStatus(Code.OK, "OK");
  }

  /**
   * Helper method to create an OK status.
   * @param message message to return
   * @return OK task status
   */
  public static DatastreamTaskStatus OK(String message) {
    return new DatastreamTaskStatus(Code.OK, message);
  }

  /**
   * Helper method to create an ERROR status
   * @param message
   * @return ERROR task status
   */
  public static DatastreamTaskStatus ERROR(String message) {
    return new DatastreamTaskStatus(Code.ERROR, message);
  }

  /**
   * @return kind of the status
   */
  public Code getCode() {
    return _code;
  }

  /**
   * @return message associated with the status
   */
  public String getMessage() {
    return _message;
  }

  @Override
  public String toString() {
    return String.format("TaskStatus: code=%s, msg=%s", _code, _message);
  }
}
