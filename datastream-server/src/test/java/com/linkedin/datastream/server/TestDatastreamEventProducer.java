package com.linkedin.datastream.server;


import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamEvent;
import com.linkedin.datastream.common.DatastreamEventRecord;
import com.linkedin.datastream.common.DatastreamTarget;
import com.linkedin.datastream.common.KafkaConnection;
import com.linkedin.datastream.common.VerifiableProperties;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TestDatastreamEventProducer {
  class ValidatingTransport implements TransportProvider {
    private int _numEvents;
    private int _numFlushes;

    public int getNumEvents() {
      return _numEvents;
    }

    public int getNumFlushes() {
      return _numFlushes;
    }

    @Override
    public void createTopic(String topicName, int numberOfPartitions, Properties topicConfig) {

    }

    @Override
    public void dropTopic(String topicName) {

    }

    @Override
    public void send(DatastreamEventRecord record) {
      ++_numEvents;
    }

    @Override
    public void flush() {
      ++_numFlushes;
    }
  }

  class ValidatingCheckpointProvider implements CheckpointProvider {
    private Map<String, String> _cpMap = new HashMap<>();

    @Override
    public String load(String taskName) {
      return _cpMap.getOrDefault(taskName, "");
    }

    @Override
    public void save(String taskName, String checkpoint) {
      _cpMap.put(taskName, checkpoint);
    }
  }

  private Datastream createDatastream() {
    Datastream datastream = new Datastream();
    datastream.setName("dummy datastream");
    datastream.setConnectorType("EspressoConnector");
    datastream.setSource("dummy source");
    StringMap metadata = new StringMap();
    datastream.setMetadata(metadata);

    Datastream.Target target = new Datastream.Target();
    KafkaConnection conn = new KafkaConnection();
    conn.setMetadataBrokers("dummy brokers");
    conn.setPartitions(10);
    conn.setTopicName("dummy topic");
    target.setKafkaConnection(conn);
    datastream.setTarget(target);

    return datastream;
  }

  private DatastreamEventRecord createEventRecord(Datastream datastream, boolean isTxnEnd, int eventSize) {
    DatastreamEvent event = new DatastreamEvent();
    event.datastream = datastream.getName();
    event.key = ByteBuffer.allocate(1);
    event.payload = ByteBuffer.allocate(1);
    return new DatastreamEventRecord(event, "dummy topic", 0, isTxnEnd, eventSize, "new dummy checkpoint");
  }

  @Test
  public void testSendWithManualFlush() {
    Datastream datastream = createDatastream();

    // Disable flushing
    datastream.getMetadata().put("flushPolicy", DatastreamEventProducer.POLICY_PERIODIC);
    datastream.getMetadata().put("flushThreshold", String.valueOf(Long.MAX_VALUE));

    DatastreamTask task = new DatastreamTask(datastream);
    task.setDatastreamName("dummy task");
    task.setDatastream(datastream);

    ValidatingTransport transport = new ValidatingTransport();
    ValidatingCheckpointProvider cpProvider = new ValidatingCheckpointProvider();
    cpProvider.save(task.getDatastreamTaskName(), "dummy checkpoint");

    VerifiableProperties config = new VerifiableProperties(new Properties());
    DatastreamEventProducer producer = new DatastreamEventProducer(task, transport, cpProvider, config);

    Assert.assertEquals(producer.getInitialCheckpoint(), "dummy checkpoint");

    DatastreamEvent event = new DatastreamEvent();
    event.datastream = datastream.getName();
    event.key = ByteBuffer.allocate(1);
    event.payload = ByteBuffer.allocate(1);
    DatastreamEventRecord record = new DatastreamEventRecord(event,
            "dummy topic", 0, false, 100, "new dummy checkpoint");

    producer.send(record);

    Assert.assertEquals(transport.getNumEvents(), 1);

    // Force flush
    try {
      Method flushMethod = DatastreamEventProducer.class.getDeclaredMethod("flush");
      flushMethod.setAccessible(true);
      flushMethod.invoke(producer);
    } catch (Exception e) {
      Assert.fail();
      return;
    }

    Assert.assertEquals(transport.getNumFlushes(), 1);
    Assert.assertEquals(producer.getLatestCheckpoint(), "new dummy checkpoint");
  }

  @Test
  public void testSendWithBytesFlush() {
    Datastream datastream = createDatastream();

    int totalBytes = 5000;
    int numEvents = 10;
    int eventSize = totalBytes / numEvents;
    int threshold = eventSize * 2;
    int numFlushes = totalBytes / threshold;

    datastream.getMetadata().put("flushPolicy", DatastreamEventProducer.POLICY_BYTES);
    datastream.getMetadata().put("flushThreshold", String.valueOf(threshold));

    DatastreamTask task = new DatastreamTask(datastream);
    task.setDatastreamName("dummy task");
    task.setDatastream(datastream);

    ValidatingTransport transport = new ValidatingTransport();
    ValidatingCheckpointProvider cpProvider = new ValidatingCheckpointProvider();
    VerifiableProperties config = new VerifiableProperties(new Properties());
    DatastreamEventProducer producer = new DatastreamEventProducer(task, transport, cpProvider, config);

    for (int i = 0; i < numEvents; i++) {
      DatastreamEventRecord record = createEventRecord(datastream, false, eventSize);
      producer.send(record);
    }

    Assert.assertEquals(transport.getNumEvents(), numEvents);
    Assert.assertEquals(transport.getNumFlushes(), numFlushes);
  }
}
