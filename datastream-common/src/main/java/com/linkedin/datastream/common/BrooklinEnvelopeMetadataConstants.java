package com.linkedin.datastream.common;

public class BrooklinEnvelopeMetadataConstants {

  public enum OpCode {
    INSERT,

    UPDATE,

    DELETE,
  }

  // Event opcode
  public static final String OPCODE = "OpCode";

  // Scn of the event
  public static final String SCN = "Scn";

  // Database to which the event belongs
  public static final String DATABASE = "Database";

  // Table for which the event belongs
  public static final String TABLE = "Table";

  // Timestamp of when the event was last modified in the source
  public static final String EVENT_TIMESTAMP = "EventTimestamp";

  // Timestamp of the event source
  public static final String SOURCE_TIMESTAMP = "SourceTimestamp";
}
