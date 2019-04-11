/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.linkedin.kafka.clients.auditing.AuditType;
import com.linkedin.kafka.clients.auditing.Auditor;


/**
 * The default no-op auditor class
 * @param <K> type of the key
 * @param <V> type of the value
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
  public void record(Object auditToken, String topic, Long timestamp, Long messageCount, Long bytesCount,
      AuditType auditType) {

  }

  @Override
  public void close(long timeout, TimeUnit unit) {

  }

  @Override
  public void close() {

  }
}