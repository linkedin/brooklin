/**
 *  Copyright 2020 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.kafka;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.mockito.invocation.Invocation;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.testutil.DatastreamTestUtils;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
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
    transportProviderProperties.put(KafkaTransportProviderAdmin.ZK_CONNECT_STRING_CONFIG, "zk-connect-string");

    String topicName = "random-topic-42";

    MockKafkaProducerWrapper<byte[], byte[]> producerWrapper =
        new MockKafkaProducerWrapper<>("log-suffix", transportProviderProperties, "metrics");

    String destinationUri = "localhost:1234/" + topicName;
    Datastream ds = DatastreamTestUtils.createDatastream("test", "ds1", "source", destinationUri, 1);

    DatastreamTask task = new DatastreamTaskImpl(Collections.singletonList(ds));
    ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topicName, null, null);
    producerWrapper.assignTask(task);

    // Sending first event, send should pass, none of the other methods on the producer should have been called
    producerWrapper.send(task, producerRecord, null);
    producerWrapper.verifySend(1);
    producerWrapper.verifyFlush(0);
    producerWrapper.verifyClose(0, 0);
    Assert.assertEquals(producerWrapper.getNumCreateKafkaProducerCalls(), 1);

    // Calling the first flush() on a separate thread because the InterruptException calls Thread interrupt() on the
    // currently running thread. If not run on a separate thread, the test thread itself will be interrupted.
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(() -> {
      // Flush has been mocked to throw an InterruptException
      Assert.assertThrows(InterruptException.class, producerWrapper::flush);
    }).get();

    producerWrapper.verifySend(1);
    producerWrapper.verifyFlush(1);
    producerWrapper.verifyClose(1, 1);

    // Second send should create a new producer, resetting flush() and close() invocation counts
    producerWrapper.send(task, producerRecord, null);
    producerWrapper.verifySend(1);
    producerWrapper.verifyFlush(0);
    producerWrapper.verifyClose(0, 1);
    Assert.assertEquals(producerWrapper.getNumCreateKafkaProducerCalls(), 2);

    // Second producer's flush() has not been mocked to throw exceptions, this should not throw
    producerWrapper.flush();
    producerWrapper.verifySend(1);
    producerWrapper.verifyFlush(1);
    producerWrapper.verifyClose(0, 1);
    Assert.assertEquals(producerWrapper.getNumCreateKafkaProducerCalls(), 2);

    // Send should reuse the older producer and the counts should not be reset
    producerWrapper.send(task, producerRecord, null);
    producerWrapper.verifySend(2);
    producerWrapper.verifyFlush(1);
    producerWrapper.verifyClose(0, 1);
    Assert.assertEquals(producerWrapper.getNumCreateKafkaProducerCalls(), 2);

    // Closing the producer's task. Since this is the only task, the producer should be closed
    producerWrapper.close(task);
    producerWrapper.verifySend(2);
    producerWrapper.verifyFlush(1);
    producerWrapper.verifyClose(1, 2);
    Assert.assertEquals(producerWrapper.getNumCreateKafkaProducerCalls(), 2);
  }

  @Test
  public void testFlushTimeout() throws Exception {
    DynamicMetricsManager.createInstance(new MetricRegistry(), getClass().getSimpleName());
    Properties transportProviderProperties = new Properties();
    transportProviderProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");
    transportProviderProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "testClient");
    transportProviderProperties.put(KafkaTransportProviderAdmin.ZK_CONNECT_STRING_CONFIG, "zk-connect-string");
    transportProviderProperties.put(KafkaProducerWrapper.CFG_PRODUCER_FLUSH_TIMEOUT_MS, "1");

    String topicName = "topic-42";

    MockKafkaProducerWrapper<byte[], byte[]> producerWrapper =
        new MockKafkaProducerWrapper<>("log-suffix", transportProviderProperties, "metrics",
            TimeoutException.class);

    String destinationUri = "localhost:1234/" + topicName;
    Datastream ds = DatastreamTestUtils.createDatastream("test", "ds1", "source", destinationUri, 1);

    DatastreamTask task = new DatastreamTaskImpl(Collections.singletonList(ds));
    ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topicName, null, null);
    producerWrapper.assignTask(task);

    // Sending first event, send should pass, none of the other methods on the producer should have been called
    producerWrapper.send(task, producerRecord, null);
    producerWrapper.verifySend(1);
    producerWrapper.verifyFlush(0);
    producerWrapper.verifyClose(0, 0);
    Assert.assertEquals(producerWrapper.getNumCreateKafkaProducerCalls(), 1);

    // Producer was mocked to throw a TimeoutException
    Assert.assertThrows(TimeoutException.class, producerWrapper::flush);

    producerWrapper.verifySend(1);
    producerWrapper.verifyFlush(1);
    producerWrapper.verifyClose(0, 0);

    // Second send should reuse the same producer since the producer is not closed on TimeoutException
    producerWrapper.send(task, producerRecord, null);
    producerWrapper.verifySend(2);
    producerWrapper.verifyFlush(1);
    producerWrapper.verifyClose(0, 0);
    Assert.assertEquals(producerWrapper.getNumCreateKafkaProducerCalls(), 1);

    // Closing the producer's task. Since this is the only task, the producer should be closed
    producerWrapper.close(task);
    producerWrapper.verifySend(2);
    producerWrapper.verifyFlush(1);
    producerWrapper.verifyClose(1, 1);
    Assert.assertEquals(producerWrapper.getNumCreateKafkaProducerCalls(), 1);
  }

  @Test
  public void testAssignAndUnassignTask() throws Exception {
    DynamicMetricsManager.createInstance(new MetricRegistry(), getClass().getSimpleName());
    Properties transportProviderProperties = new Properties();
    transportProviderProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");
    transportProviderProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "testClient");
    transportProviderProperties.put(KafkaTransportProviderAdmin.ZK_CONNECT_STRING_CONFIG, "zk-connect-string");
    transportProviderProperties.put(KafkaProducerWrapper.CFG_PRODUCER_FLUSH_TIMEOUT_MS, "1");

    String topicName = "topic-43";

    MockKafkaProducerWrapper<byte[], byte[]> producerWrapper =
        new MockKafkaProducerWrapper<>("log-suffix", transportProviderProperties, "metrics",
            TimeoutException.class);

    String destinationUri = "localhost:1234/" + topicName;
    Datastream ds = DatastreamTestUtils.createDatastream("test", "ds1", "source", destinationUri, 1);

    DatastreamTask task = new DatastreamTaskImpl(Collections.singletonList(ds));
    ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topicName, null, null);
    producerWrapper.assignTask(task);

    // Sending first event, send should pass, none of the other methods on the producer should have been called
    producerWrapper.send(task, producerRecord, null);
    producerWrapper.verifySend(1);
    producerWrapper.verifyFlush(0);
    producerWrapper.verifyClose(0, 0);
    Assert.assertEquals(producerWrapper.getNumCreateKafkaProducerCalls(), 1);

    producerWrapper.unassignTask(task);
    producerWrapper.verifySend(1);
    producerWrapper.verifyFlush(0);
    producerWrapper.verifyClose(1, 1);

    // Second send should fail as the task is unassigned
    Assert.assertThrows(DatastreamRuntimeException.class, () -> producerWrapper.send(task, producerRecord, null));
    producerWrapper.verifySend(1);
    producerWrapper.verifyFlush(0);
    producerWrapper.verifyClose(1, 2);

    // Closing the producer's task. Since this is the only task, the producer should be closed
    producerWrapper.close(task);
    producerWrapper.verifySend(1);
    producerWrapper.verifyFlush(0);
    producerWrapper.verifyClose(1, 2);
    Assert.assertEquals(producerWrapper.getNumCreateKafkaProducerCalls(), 1);

    producerWrapper.assignTask(task);
    // Sending first event, send should pass, none of the other methods on the producer should have been called
    producerWrapper.send(task, producerRecord, null);
    producerWrapper.verifySend(1);
    producerWrapper.verifyFlush(0);
    producerWrapper.verifyClose(0, 2);
    Assert.assertEquals(producerWrapper.getNumCreateKafkaProducerCalls(), 2);
  }

  private static class MockKafkaProducerWrapper<K, V> extends KafkaProducerWrapper<K, V> {
    private Class<? extends Throwable> _exceptionClass;
    private boolean _createKafkaProducerCalled;
    private int _numCreateKafkaProducerCalls;
    private int _numShutdownProducerCalls;
    private Producer<K, V> _mockProducer;

    MockKafkaProducerWrapper(String logSuffix, Properties props, String metricsNamesPrefix) {
      this(logSuffix, props, metricsNamesPrefix, InterruptException.class);
    }

    MockKafkaProducerWrapper(String logSuffix, Properties props, String metricsNamesPrefix,
        Class<? extends Throwable> exceptionClass) {
      super(logSuffix, props, metricsNamesPrefix);
      _exceptionClass = exceptionClass;
    }

    @Override
    Producer<K, V> createKafkaProducer() {
      @SuppressWarnings("unchecked")
      Producer<K, V> producer = (Producer<K, V>) mock(Producer.class);
      // Calling flush() on the first producer created will throw an exception of type _exceptionClass.
      if (!_createKafkaProducerCalled) {
        doThrow(_exceptionClass).when(producer).flush(anyInt(), any(TimeUnit.class));
      }

      _mockProducer = producer;
      _createKafkaProducerCalled = true;
      ++_numCreateKafkaProducerCalls;
      return _mockProducer;
    }

    @Override
    void shutdownProducer() {
      super.shutdownProducer();
      ++_numShutdownProducerCalls;
    }

    void verifySend(int numExpected) {
      verify(_mockProducer, times(numExpected)).send(any(), any(Callback.class));
    }

    void verifyFlush(int numExpected) {
      verify(_mockProducer, times(numExpected)).flush(anyInt(), any(TimeUnit.class));
    }

    void verifyClose(int numExpectedClose, int numExpectedShutdownProducerCalls) throws NoSuchMethodException {
      // Producer close is invoked in a separate thread. Must wait for the thread to get scheduled and call close
      Method method = Producer.class.getMethod("close", long.class, TimeUnit.class);
      PollUtils.poll(() -> {
        Collection<Invocation> invocations = mockingDetails(_mockProducer).getInvocations();
        long count = invocations.stream().filter(invocation -> invocation.getMethod().equals(method)).count();
        return count == numExpectedClose;
      }, 1000, 10000);
      verify(_mockProducer, times(numExpectedClose)).close(anyLong(), any(TimeUnit.class));

      Assert.assertEquals(_numShutdownProducerCalls, numExpectedShutdownProducerCalls);
    }

    public int getNumCreateKafkaProducerCalls() {
      return _numCreateKafkaProducerCalls;
    }
  }
}
