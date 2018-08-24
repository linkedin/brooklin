package com.linkedin.datastream.connectors.kafka;

import com.linkedin.kafka.clients.auditing.AuditType;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.linkedin.kafka.clients.auditing.Auditor;

/**
 * The default no-op auditor class.
 */
public class NoOpAuditor<K, V> implements Auditor<K, V> {

  @Override
  public void configure(Map<String, ?> configs) {

  }

  @Override
  public void start() {

  }

  @Override
  public Object auditToken(K key, V value) {
    return null;
  }

  @Override
  public void record(Object auditToken,
      String topic,
      Long timestamp,
      Long messageCount,
      Long bytesCount,
      AuditType auditType) {

  }


  @Override
  public void close(long timeout, TimeUnit unit) {

  }

  @Override
  public void close() {

  }
}