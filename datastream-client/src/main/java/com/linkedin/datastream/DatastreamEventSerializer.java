package com.linkedin.datastream;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import com.linkedin.datastream.common.DatastreamEvent;


/**
 * Datastream event serializer class that helps serialize and deserialize
 * Datastream Event.
 * Along with the serialization, This also ensures that the schemas are registered
 */
public class DatastreamEventSerializer implements Deserializer<DatastreamEvent>, Serializer<DatastreamEvent> {

  private SchemaRegistry _registry;

  public void setSchemaRegistry(SchemaRegistry registry) {
    _registry = registry;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {

  }

  @Override
  public byte[] serialize(String topic, DatastreamEvent data) {
    return new byte[0];
  }

  @Override
  public DatastreamEvent deserialize(String topic, byte[] data) {
    return null;
  }

  @Override
  public void close() {

  }
}
