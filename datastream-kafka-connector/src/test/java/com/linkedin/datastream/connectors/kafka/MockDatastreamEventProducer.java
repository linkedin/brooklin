package com.linkedin.datastream.connectors.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.server.DatastreamEventProducer;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.api.transport.DatastreamRecordMetadata;
import com.linkedin.datastream.server.api.transport.SendCallback;


public class MockDatastreamEventProducer implements DatastreamEventProducer {

  private static final Logger LOG = LoggerFactory.getLogger(MockDatastreamEventProducer.class);
  private final List<DatastreamProducerRecord> _events = Collections.synchronizedList(new ArrayList<>());
  private int _numFlushes = 0;
  private ExecutorService _executorService = Executors.newFixedThreadPool(1);
  private Duration _callbackThrottleDuration;
  private Predicate<DatastreamProducerRecord> _sendFailCondition;
  private Duration _flushDuration;

  public MockDatastreamEventProducer() {
    this(null, null, null);
  }

  public MockDatastreamEventProducer(Predicate<DatastreamProducerRecord> sendFailCondition) {
    this(null, sendFailCondition, null);
  }

  public MockDatastreamEventProducer(Duration callbackThrottleDuration) {
    this(callbackThrottleDuration, null, null);
  }

  public MockDatastreamEventProducer(Duration callbackThrottleDuration,
      Predicate<DatastreamProducerRecord> sendFailCondition, Duration flushDuration) {
    _callbackThrottleDuration = callbackThrottleDuration;
    _sendFailCondition = sendFailCondition;
    _flushDuration = flushDuration;
  }

  @Override
  public void send(DatastreamProducerRecord event, SendCallback callback) {
    if (_sendFailCondition != null && _sendFailCondition.test(event)) {
      throw new DatastreamRuntimeException("Random exception");
    }

    if (_callbackThrottleDuration != null) {
      // throttle send callbacks by sleeping for specified duration before sending event
      _executorService.submit(() -> {
        try {
          if (!_callbackThrottleDuration.isZero()) {
            Thread.sleep(_callbackThrottleDuration.toMillis());
          }
          sendEvent(event, callback);
        } catch (InterruptedException e) {
          LOG.error("Sleep was interrupted while throttling send callback");
          throw new DatastreamRuntimeException("Sleep was interrupted", e);
        }
      });
    } else {
      sendEvent(event, callback);
    }
  }

  private void sendEvent(DatastreamProducerRecord event, SendCallback callback) {
    _events.add(event);
    LOG.info("sent event {}, total _events {}", event, _events.size());
    DatastreamRecordMetadata md = new DatastreamRecordMetadata(event.getCheckpoint(), "mock topic", 666);
    if (callback != null) {
      callback.onCompletion(md, null);
    }
  }

  @Override
  public void flush() {
    if (_flushDuration != null && !_flushDuration.isZero()) {
      try {
        Thread.sleep(_flushDuration.toMillis());
      } catch (InterruptedException e) {
        LOG.info("Flush interrupted");
        return;
      }
    }
    _numFlushes++;
  }

  public List<DatastreamProducerRecord> getEvents() {
    return _events;
  }

  public int getNumFlushes() {
    return _numFlushes;
  }

  public void updateSendFailCondition(Predicate<DatastreamProducerRecord> sendFailCondition) {
    _sendFailCondition = sendFailCondition;
  }
}
