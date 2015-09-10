package com.linkedin.datastream.server;

/**
 * This class represents the validation result of a Datastream requested by DMS to Connector.
 */
public class DatastreamValidationResult {
  private final boolean _success;
  private final String _errorMsg;

  public DatastreamValidationResult() {
    this(true, "");
  }

  public DatastreamValidationResult(String errorMsg) {
    this(false, errorMsg);
  }

  public DatastreamValidationResult(boolean success, String errorMsg) {
    assert success || errorMsg != "";
    _success = success;
    _errorMsg = errorMsg;
  }

  /**
   * @return whether the validation is successful.
   */
  public boolean getSuccess() {
    return _success;
  }

  /**
   * @return detailed error message if unsuccessful.
   */
  public String getErrorMsg() {
    return _errorMsg;
  }
}
