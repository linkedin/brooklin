/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
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
