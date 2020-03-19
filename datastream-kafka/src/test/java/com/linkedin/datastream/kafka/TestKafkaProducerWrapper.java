/**
 *  Copyright 2020 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.kafka;

import java.util.Collections;
import java.util.Properties;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.testutil.DatastreamTestUtils;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


/**
 * Tests for {@link KafkaProducerWrapper}
 */
@Test
public class TestKafkaProducerWrapper {

  @Test
  public void testFlushInterrupt() throws Exception {
    DynamicMetricsManager.createInstance(new MetricRegistry(), getClass().getSimpleName());
    Properties transportProviderProperties = new Properties();
    transportProviderProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");
    transportProviderProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "testClient");
    transportProviderProperties.put(KafkaTransportProviderAdmin.ZK_CONNECT_STRING_CONFIG, "zkconnectstring");

    String topicName = "random-topic-42";

    MockKafkaProducerWrapper producerWrapper =
        new MockKafkaProducerWrapper("suffix", transportProviderProperties, "prefix");

    String destinationUri = "localhost:1234/" + topicName;
    Datastream ds = DatastreamTestUtils.createDatastream("test", "ds1", "source", destinationUri, 1);

    DatastreamTask task = new DatastreamTaskImpl(Collections.singletonList(ds));
    ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topicName, null, null);
    producerWrapper.assignTask(task);

    // Sending first event, send should pass, none of the other methods on the producer should have been called
    producerWrapper.send(task, producerRecord, null);
    producerWrapper.verifySend(1);
    producerWrapper.verifyFlush(0);
    producerWrapper.verifyClose(0);
    Assert.assertEquals(producerWrapper.numCreateKafkaProducerCalls, 1);

    ExecutorService executorService = Executors.newCachedThreadPool();
    executorService.submit(() -> {
      // Flush has been mocked to throw an InterruptException
      try {
        producerWrapper.flush();
        Assert.fail("Flush should have been mocked to throw an exception");
      } catch (InterruptException e) {

      }
    }).get();

    producerWrapper.verifySend(1);
    producerWrapper.verifyFlush(1);
    producerWrapper.verifyClose(1);

    // Second send should create a new producer, resetting flush() and close() invocations count
    producerWrapper.send(task, producerRecord, null);
    producerWrapper.verifySend(1);
    producerWrapper.verifyFlush(0);
    producerWrapper.verifyClose(0);
    Assert.assertEquals(producerWrapper.numCreateKafkaProducerCalls, 2);

    // Second producer's flush() has not been mocked to throw exceptions, this should not throw
    producerWrapper.flush();
    producerWrapper.verifySend(1);
    producerWrapper.verifyFlush(1);
    producerWrapper.verifyClose(0);
    Assert.assertEquals(producerWrapper.numCreateKafkaProducerCalls, 2);

    // Send should reuse the older producer and the counts should not be reset
    producerWrapper.send(task, producerRecord, null);
    producerWrapper.verifySend(2);
    producerWrapper.verifyFlush(1);
    producerWrapper.verifyClose(0);
    Assert.assertEquals(producerWrapper.numCreateKafkaProducerCalls, 2);

    // Closing the producer's task, and since this is the only task, the producer should be closed
    producerWrapper.close(task);
    producerWrapper.verifySend(2);
    producerWrapper.verifyFlush(1);
    producerWrapper.verifyClose(1);
    Assert.assertEquals(producerWrapper.numCreateKafkaProducerCalls, 2);
  }

  private static class MockKafkaProducerWrapper extends KafkaProducerWrapper<byte[], byte[]> {
    boolean createKafkaProducerCalled;
    int numCreateKafkaProducerCalls;
    Producer<byte[], byte[]> mockProducer;

    MockKafkaProducerWrapper(String logSuffix, Properties props, String metricsNamesPrefix) {
      super(logSuffix, props, metricsNamesPrefix);

      createKafkaProducerCalled = false;
      numCreateKafkaProducerCalls = 0;
    }

    @Override
    Producer<byte[], byte[]> createKafkaProducer() {
      @SuppressWarnings("unchecked")
      Producer<byte[], byte[]> producer = (Producer<byte[], byte[]>) mock(Producer.class);
      if (!createKafkaProducerCalled) {
        doThrow(new InterruptException("Interrupting flush")).when(producer).flush();
      }
      mockProducer = producer;
      createKafkaProducerCalled = true;
      ++numCreateKafkaProducerCalls;
      return mockProducer;
    }

    void verifySend(int numExpected) {
      verify(mockProducer, times(numExpected)).send(anyObject(), anyObject());
    }

    void verifyFlush(int numExpected) {
      verify(mockProducer, times(numExpected)).flush();
    }

    void verifyClose(int numExpected) {
      verify(mockProducer, times(numExpected)).close(anyInt(), anyObject());
    }
  }
}
