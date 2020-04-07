/**
 *  Copyright 2020 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.kafka;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.InterruptException;

import com.linkedin.datastream.common.CompletableFutureUtils;
import com.linkedin.datastream.common.VerifiableProperties;

/**
 * An extension of {@link KafkaProducerWrapper} with configurable timeouts for flush and send calls
 */
class BoundedKafkaProducerWrapper<K, V> extends KafkaProducerWrapper<K, V> {
  private static final long DEFAULT_SEND_TIME_OUT_MS = Duration.ofSeconds(5).toMillis();
  private static final long DEFAULT_FLUSH_TIME_OUT_MS = Duration.ofMinutes(10).toMillis();

  private static final String SEND_TIMEOUT_CONFIG_KEY = "sendTimeout";
  private static final String FLUSH_TIMEOUT_CONFIG_KEY = "flushTimeout";

  private long _sendTimeoutMs;
  private long _flushTimeoutMs;

  BoundedKafkaProducerWrapper(String logSuffix, Properties props, String metricsNamesPrefix) {
    super(logSuffix, props, metricsNamesPrefix);

    VerifiableProperties properties = new VerifiableProperties(props);
    _sendTimeoutMs = properties.getLong(SEND_TIMEOUT_CONFIG_KEY, DEFAULT_SEND_TIME_OUT_MS);
    _flushTimeoutMs = properties.getLong(FLUSH_TIMEOUT_CONFIG_KEY, DEFAULT_FLUSH_TIME_OUT_MS);
  }

  @Override
  void doSend(Producer<K, V> producer, ProducerRecord<K, V> record, Callback callback) {
    CompletableFutureUtils.within(produceMessage(producer, record), Duration.ofMillis(_sendTimeoutMs))
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
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> future = executor.submit(super::flush);

    try {
      future.get(_flushTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException ex) {
      _log.warn("Flush call timed out after {}ms. Cancelling flush", _flushTimeoutMs);
      future.cancel(true);
    } catch (ExecutionException ex) {
      Throwable cause = ex.getCause();

      if (cause instanceof InterruptException) {
        throw (InterruptException) cause;
      } else {
        // This shouldn't happen
        _log.warn("Flush failed.", cause);
      }
    } catch (InterruptedException ex) {
      // This also shouldn't happen because kafka flush use their own InterruptException
      Thread.currentThread().interrupt();
      throw new RuntimeException(ex);
    }
  }
}

