package com.linkedin.datastream.connectors.kafka;

import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamEvent;
import com.linkedin.datastream.common.DatastreamEventMetadata;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.server.DatastreamEventProducer;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.DatastreamProducerRecordBuilder;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.Pair;
import com.linkedin.datastream.server.api.transport.DatastreamRecordMetadata;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaConnectorTask implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaConnectorTask.class);

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

  public KafkaConnectorTask(DatastreamTask task, long commitIntervalMillis) {
    _task = task;
    _offsetCommitInterval = commitIntervalMillis;
  }

  @Override
  public void run() {
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
      props.put("auto.offset.reset", "latest"); //start from latest if no saved offsets
      props.put("key.deserializer", ByteArrayDeserializer.class.getCanonicalName());
      props.put("value.deserializer", ByteArrayDeserializer.class.getCanonicalName());

      try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
        _lastCommittedOffsets = System.currentTimeMillis();
        consumer.subscribe(Collections.singletonList(srcConnString.getTopicName()));

        ConsumerRecords<byte[], byte[]> records;
        while (!_shouldDie) {

          //read a batch of records
          records = consumer.poll(pollInterval);

          if (startingUp) { //bootup completes on 1st successful poll
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
            //TODO - wait? backoff? max retries?!
          }
          _workSinceLastCommit = true;

          //potentially commit our offsets (if its been long enough and we succeeded sending)
          maybeCommitOffsets(consumer, false);
        }
        maybeCommitOffsets(consumer, true);
      }
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

  private void maybeCommitOffsets(KafkaConsumer<byte[], byte[]> consumer, boolean force) {
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

  private boolean translateAndSendBatch(ConsumerRecords<byte[], byte[]> records, long readTime, String strReadTime) {
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
        LOG.error("sending event failed. metedata: {}", mdExceptionPair.getKey(), e);
      } else {
        //this is if the exception did not originate from the callback
        LOG.error("sending event failed. no metadata", e);
      }
    }
    return false;
  }

  private DatastreamProducerRecord translate(ConsumerRecord<byte[], byte[]> fromKafka, long readTime, String strReadTime) {
    DatastreamEvent event = new DatastreamEvent();
    event.key = ByteBuffer.wrap(fromKafka.key());
    event.payload = ByteBuffer.wrap(fromKafka.value());
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
}
