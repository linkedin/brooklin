/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.kafka;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TimeoutException;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;

import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.testutil.BaseKafkaZkTest;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link KafkaProducerWrapper}.
 */
public class TestKafkaProducerWrapper extends BaseKafkaZkTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestKafkaProducerWrapper.class);

  private Properties _transportProviderProperties;

  @BeforeMethod
  public void setup() {
    DynamicMetricsManager.createInstance(new MetricRegistry(), getClass().getSimpleName());
    _transportProviderProperties = new Properties();
    _transportProviderProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, _kafkaCluster.getBrokers());
    _transportProviderProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "testClient");
    _transportProviderProperties.put(KafkaTransportProviderAdmin.ZK_CONNECT_STRING_CONFIG, _kafkaCluster.getZkConnection());
  }

  @Test(expectedExceptions = TimeoutException.class)
  public void testFlushWithTimeoutFail() {
    _transportProviderProperties.put(KafkaProducerWrapper.CFG_FLUSH_TIMEOUT_MS, "1");

    KafkaProducerWrapper<byte[], byte[]> kafkaProducerWrapper =
        new KafkaProducerWrapper<>("test", _transportProviderProperties, "test");

    @SuppressWarnings("unchecked")
    Producer<byte[], byte[]> mockProducer = (Producer<byte[], byte[]>) mock(Producer.class);
    doAnswer((Answer<Void>) invocation -> {
      TimeUnit.MILLISECONDS.sleep(50);
      return null;
    }).when(mockProducer).flush();

    kafkaProducerWrapper.setProducer(mockProducer);
    kafkaProducerWrapper.flush();
  }

  @Test
  public void testFlushWithoutTimeoutPass() {

    KafkaProducerWrapper<byte[], byte[]> kafkaProducerWrapper =
        new KafkaProducerWrapper<>("test", _transportProviderProperties, "test");

    @SuppressWarnings("unchecked")
    Producer<byte[], byte[]> mockProducer = (Producer<byte[], byte[]>) mock(Producer.class);
    doAnswer((Answer<Void>) invocation -> {
      TimeUnit.MILLISECONDS.sleep(50);
      return null;
    }).when(mockProducer).flush();

    kafkaProducerWrapper.setProducer(mockProducer);
    kafkaProducerWrapper.flush();
  }

  @Test
  public void testFlushWithTimeout() {
    _transportProviderProperties.put(KafkaProducerWrapper.CFG_FLUSH_TIMEOUT_MS, "100");

    KafkaProducerWrapper<byte[], byte[]> kafkaProducerWrapper =
        new KafkaProducerWrapper<>("test", _transportProviderProperties, "test");

    @SuppressWarnings("unchecked")
    Producer<byte[], byte[]> mockProducer = (Producer<byte[], byte[]>) mock(Producer.class);
    doAnswer((Answer<Void>) invocation -> {
      TimeUnit.MILLISECONDS.sleep(50);
      return null;
    }).when(mockProducer).flush();

    kafkaProducerWrapper.setProducer(mockProducer);
    kafkaProducerWrapper.flush();
  }

}
