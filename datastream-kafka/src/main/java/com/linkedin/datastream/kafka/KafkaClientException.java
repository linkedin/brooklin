/**
 *  Copyright 2020 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */

package com.linkedin.datastream.kafka;
import org.apache.kafka.clients.producer.RecordMetadata;

class KafkaClientException extends Exception {
  private static final long serialVersionUID = 1;

  private final RecordMetadata _metadata;

  public RecordMetadata getMetadata() {
    return _metadata;
  }

  public KafkaClientException(RecordMetadata metadata, Throwable innerException) {
    super(innerException);
    _metadata = metadata;
  }
}