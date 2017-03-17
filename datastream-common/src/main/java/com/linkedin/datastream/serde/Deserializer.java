package com.linkedin.datastream.serde;

/**
 * DeSerializer interface for brooklin which is used to convert bytes to object.
 */
public interface Deserializer {

  /**
   * Deserializes given serialized object from an array of bytes to its original form.
   * @param data Array of bytes representing serialized object.
   * @return Original deserialized object.
   */
  Object deserialize(byte[] data);
}
