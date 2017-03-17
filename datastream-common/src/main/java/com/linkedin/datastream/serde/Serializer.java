package com.linkedin.datastream.serde;

/**
 * Serializer interface for brooklin which is used to convert object to bytes before they are
 * written to the transport.
 */
public interface Serializer {

  /**
   * Serializes given object to an array of bytes.
   * @param object Object of specific type to serialize.
   * @return An array of bytes representing the object in serialized form.
   */
  byte[] serialize(Object object);
}
