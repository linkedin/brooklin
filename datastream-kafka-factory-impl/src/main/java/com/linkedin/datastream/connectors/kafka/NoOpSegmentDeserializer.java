package com.linkedin.datastream.connectors.kafka;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.linkedin.kafka.clients.largemessage.LargeMessageSegment;

public class NoOpSegmentDeserializer implements Deserializer<LargeMessageSegment> {
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public LargeMessageSegment deserialize(String s, byte[] bytes) {
    return null;
  }

  @Override
  public void close() {

  }
}
