/**
 *  Copyright 2020 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.kafka;

import java.time.Duration;
import java.util.Properties;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.InterruptException;

import com.linkedin.datastream.common.CompletableFutureUtils;
import com.linkedin.datastream.common.VerifiableProperties;

/**
 * An extension of {@link KafkaProducerWrapper} with bounded calls for flush and send
 */
class BoundedKafkaProducerWrapper<K, V> extends KafkaProducerWrapper<K, V> {
  private static final int DEFAULT_SEND_TIME_OUT = 5000;
  private static final int DEFAULT_FLUSH_TIME_OUT = 10 * 60 * 1000;

  private static final String SEND_TIMEOUT_CONFIG_KEY = "brooklin.server.kafkaProducerWrapper.sendTimeout";
  private static final String FLUSH_TIMEOUT_CONFIG_KEY = "brooklin.server.kafkaProducerWrapper.flushTimeout";

  private int _sendTimeout;
  private int _flushTimeout;

  BoundedKafkaProducerWrapper(String logSuffix, Properties props, String metricsNamesPrefix) {
    super(logSuffix, props, metricsNamesPrefix);

    VerifiableProperties properties = new VerifiableProperties(props);
    _sendTimeout = properties.getInt(SEND_TIMEOUT_CONFIG_KEY, DEFAULT_SEND_TIME_OUT);
    _flushTimeout = properties.getInt(FLUSH_TIMEOUT_CONFIG_KEY, DEFAULT_FLUSH_TIME_OUT);
  }

  @Override
  void doSend(Producer<K, V> producer, ProducerRecord<K, V> record, Callback callback) {
    CompletableFutureUtils.within(produceMessage(producer, record), Duration.ofMillis(_sendTimeout))
        .thenAccept(m -> callback.onCompletion(m, null))
        .exceptionally(completionEx -> {
          Throwable cause = completionEx.getCause();
          if (cause instanceof KafkaClientException) {
            KafkaClientException ex = (KafkaClientException) cause;
            callback.onCompletion(ex.getMetadata(), (Exception) ex.getCause());
          } else if (cause instanceof java.util.concurrent.TimeoutException) {
            _log.warn("KafkaProducerWrapper send timed out. The destination topic may be unavailable.");
            callback.onCompletion(null, (java.util.concurrent.TimeoutException) cause);
          }
          return null;
        });
  }

  private CompletableFuture<RecordMetadata> produceMessage(Producer<K, V> producer, ProducerRecord<K, V> record) {
    CompletableFuture<RecordMetadata> future = new CompletableFuture<>();

    producer.send(record, (metadata, exception) -> {
      if (exception == null) {
        future.complete(metadata);
      } else {
        future.completeExceptionally(new KafkaClientException(metadata, exception));
      }
    });

    return future;
  }

  @Override
  synchronized void flush() {
    if (_kafkaProducer != null) {
      try {
        CompletableFutureUtils.within(CompletableFuture.runAsync(() -> _kafkaProducer.flush()),
            Duration.ofMillis(_flushTimeout)).join();
      } catch (CompletionException e) {
        Throwable cause = e.getCause();

        if (cause instanceof InterruptException) {
          // The KafkaProducer object should not be reused on an interrupted flush
          _log.warn("Kafka producer flush interrupted, closing producer {}.", _kafkaProducer);
          shutdownProducer();
          throw (InterruptException) cause;
        } else if (cause instanceof java.util.concurrent.TimeoutException) {
          _log.warn("Kafka producer flush timed out after {}ms. Destination topic may be unavailable.", _flushTimeout);
        }

        throw e;
      }
    }
  }
}
