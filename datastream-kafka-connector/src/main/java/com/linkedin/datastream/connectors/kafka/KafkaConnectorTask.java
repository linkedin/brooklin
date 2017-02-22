package com.linkedin.datastream.connectors.kafka;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.AvroUtils;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamEvent;
import com.linkedin.datastream.common.DatastreamEventMetadata;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.server.DatastreamEventProducer;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.DatastreamProducerRecordBuilder;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.Pair;
import com.linkedin.datastream.server.api.transport.DatastreamRecordMetadata;


public class KafkaConnectorTask implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaConnectorTask.class);
  private final KafkaConsumerFactory<?, ?> _consumerFactory;
  private final Properties _consumerProps;

  //lifecycle
  private volatile boolean _shouldDie = false;
  private final CountDownLatch _startedLatch = new CountDownLatch(1);
  private final CountDownLatch _stoppedLatch = new CountDownLatch(1);
  //config
  private final DatastreamTask _task;
  private final long _offsetCommitInterval;
  //state
  private volatile Thread _thread;
  private volatile String _taskName;
  private String _srcValue;
  private DatastreamEventProducer _producer;
  private long _lastCommittedOffsets;
  private boolean _workSinceLastCommit; //since last offset commit

  public KafkaConnectorTask(KafkaConsumerFactory<?, ?> factory, Properties consumerProps, DatastreamTask task,
      long commitIntervalMillis) {
    LOG.info("Creating kafka connector task for datastream task {} with commit interval {}Ms", task,
        commitIntervalMillis);
    _consumerProps = consumerProps;
    _consumerFactory = factory;
    _task = task;
    _offsetCommitInterval = commitIntervalMillis;
  }

  @Override
  public void run() {
    LOG.info("Starting the kafka connector task for {}", _task);
    boolean startingUp = true;
    long pollInterval = 0; //so 1st call to poll is fast for purposes of startup
    _thread = Thread.currentThread();
    try {

      DatastreamSource source = _task.getDatastreamSource();
      KafkaConnectionString srcConnString = KafkaConnectionString.valueOf(source.getConnectionString());
      StringJoiner csv = new StringJoiner(",");
      srcConnString.getBrokers().forEach(broker -> csv.add(broker.toString()));
      String bootstrapValue = csv.toString();
      _srcValue = srcConnString.toString();

      DatastreamDestination destination = _task.getDatastreamDestination();
      String dstConnString = destination.getConnectionString();
      _producer = _task.getEventProducer();
      _taskName = srcConnString + "-to-" + dstConnString;

      Properties props = new Properties();
      props.put("bootstrap.servers", bootstrapValue);
      props.put("group.id", _taskName);
      props.put("enable.auto.commit", "false"); //auto-commits are unsafe
      // TODO this is problematic. if the connector falls of the topic, it automatically starts from latest resulting
      // in data loss.
      props.put("auto.offset.reset", "latest"); //start from latest if no saved offsets
      props.putAll(_consumerProps);

      try (Consumer<?, ?> consumer = _consumerFactory.createConsumer(props)) {
        _lastCommittedOffsets = System.currentTimeMillis();
        consumer.subscribe(Collections.singletonList(srcConnString.getTopicName()));

        ConsumerRecords<?, ?> records;
        while (!_shouldDie) {

          //read a batch of records
          records = consumer.poll(pollInterval);

          if (startingUp) { // bootup completes on 1st successful poll
            pollInterval = _offsetCommitInterval / 2; //leave time for processing, assume 50-50
            startingUp = false;
            _startedLatch.countDown();
          }

          if (records.count() == 0) {
            maybeCommitOffsets(consumer, false);
            continue;
          }
          long readTime = System.currentTimeMillis();
          String strReadTime = String.valueOf(readTime);

          //send the batch out the other end

          while (!_shouldDie) {
            boolean success = translateAndSendBatch(records, readTime, strReadTime);
            if (success) {
              break;
            }

            LOG.warn("Sending the batch of events failed for datastream task {}. Retrying", _task);
            //TODO - wait? backoff? max retries?!
          }
          _workSinceLastCommit = true;

          //potentially commit our offsets (if its been long enough and we succeeded sending)
          maybeCommitOffsets(consumer, false);
        }
        maybeCommitOffsets(consumer, true);
      }
    } catch (Exception e) {
      LOG.error("{} failed with exception.", _taskName, e);
      throw e;
    } finally {
      _stoppedLatch.countDown();
      LOG.info("{} stopped", _taskName);
    }
  }

  public void stop() {
    LOG.info("{} stopping", _taskName);
    _shouldDie = true;
    _thread.interrupt();
  }

  public boolean awaitStart(long timeout, TimeUnit unit) throws InterruptedException {
    return _startedLatch.await(timeout, unit);
  }

  public boolean awaitStop(long timeout, TimeUnit unit) throws InterruptedException {
    return _stoppedLatch.await(timeout, unit);
  }

  private void maybeCommitOffsets(Consumer<?, ?> consumer, boolean force) {
    if (!_workSinceLastCommit) {
      return;
    }
    long now = System.currentTimeMillis();
    long timeSinceLastCommit = now - _lastCommittedOffsets;
    if (timeSinceLastCommit > _offsetCommitInterval || force) {
      _producer.flush();
      consumer.commitSync();
      _lastCommittedOffsets = now;
      _workSinceLastCommit = false;
    }
  }

  private boolean translateAndSendBatch(ConsumerRecords<?, ?> records, long readTime, String strReadTime) {
    AtomicReference<Pair<DatastreamRecordMetadata, Exception>> failure = new AtomicReference<>(null);
    try {
      CountDownLatch doneLatch = new CountDownLatch(1);
      AtomicInteger numLeft = new AtomicInteger(records.count());
      records.forEach(record -> {
        DatastreamProducerRecord translated = translate(record, readTime, strReadTime);
        _producer.send(translated, (metadata, exception) -> {
          if (exception == null) {
            //success
            if (numLeft.decrementAndGet() == 0) {
              doneLatch.countDown(); //all done (successfully)
            }
          } else {
            //failure
            failure.set(new Pair<>(metadata, exception));
            doneLatch.countDown();
          }
        });
      });

      // TODO This behavior seems problematic. Right now kafka connector acts in a synchronous manner.
      // i.e. poll() -> translateAndSend() -> Wait for send to complete -> poll() again
      doneLatch.await();
      Pair<DatastreamRecordMetadata, Exception> mdExceptionPair = failure.get();
      if (mdExceptionPair == null) {
        //successfully sent out the entire batch
        return true;
      }
      throw mdExceptionPair.getValue();
    } catch (Exception e) {
      //some part of the batch failed to send out, or (if pair is null) some other exception
      Pair<DatastreamRecordMetadata, Exception> mdExceptionPair = failure.get();
      if (mdExceptionPair != null) {
        LOG.error("sending event failed. metadata: {}", mdExceptionPair.getKey(), e);
      } else {
        //this is if the exception did not originate from the callback
        LOG.error("sending event failed. no metadata", e);
      }
    }
    return false;
  }

  private DatastreamProducerRecord translate(ConsumerRecord<?, ?> fromKafka, long readTime, String strReadTime) {
    DatastreamEvent event = new DatastreamEvent();
    event.key = getSerializedBytes(fromKafka.key());
    event.payload = getSerializedBytes(fromKafka.value());
    event.previous_payload = null;
    event.metadata = new HashMap<>();
    event.metadata.put("kafka-origin", _srcValue);
    int partition = fromKafka.partition();
    String partitionStr = String.valueOf(partition);
    event.metadata.put("kafka-origin-partition", partitionStr);
    String offsetStr = String.valueOf(fromKafka.offset());
    event.metadata.put("kafka-origin-offset", offsetStr);
    event.metadata.put(DatastreamEventMetadata.EVENT_TIMESTAMP, strReadTime);
    //TODO - copy over headers if/when they are ever supported
    DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
    builder.addEvent(event);
    builder.setEventsSourceTimestamp(readTime);
    builder.setPartition(partition); //assume source partition count is same as dest
    builder.setSourceCheckpoint(partitionStr + "-" + offsetStr);
    return builder.build();
  }

  private ByteBuffer getSerializedBytes(Object field) {
    if (field instanceof byte[]) {
      return ByteBuffer.wrap((byte[]) field);
    } else if (field instanceof String) {
      return ByteBuffer.wrap(((String) field).getBytes());
    } else if (field instanceof IndexedRecord) {
      IndexedRecord record = (IndexedRecord) field;
      try {
        return ByteBuffer.wrap(AvroUtils.encodeAvroIndexedRecord(record.getSchema(), record));
      } catch (IOException e) {
        throw new DatastreamRuntimeException("Unable to serialzie the avro record", e);
      }
    } else {
      throw new DatastreamRuntimeException("Kafka connector doesn't support field of type" + field.getClass());
    }
  }
}
