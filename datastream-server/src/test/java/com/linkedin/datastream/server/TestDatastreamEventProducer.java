package com.linkedin.datastream.server;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamEvent;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.server.providers.CheckpointProvider;
import com.linkedin.datastream.server.api.transport.TransportException;
import com.linkedin.datastream.server.api.transport.TransportProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

class InMemoryCheckpointProvider implements CheckpointProvider {
  private Map<DatastreamTask, String> _cpMap = new HashMap<>();

  @Override
  public void commit(Map<DatastreamTask, String> checkpoints) {
    if (checkpoints.size() != 0) {
      _cpMap.putAll(checkpoints);
    }
  }

  @Override
  public Map<DatastreamTask, String> getCommitted(List<DatastreamTask> datastreamTasks) {
    Map<DatastreamTask, String> ret = new HashMap<>();
    for (DatastreamTask task: datastreamTasks) {
      if (_cpMap.containsKey(task)) {
        ret.put(task, _cpMap.get(task));
      }
    }
    return ret;
  }
}

public class TestDatastreamEventProducer {
  private static final Logger LOG = LoggerFactory.getLogger(TestDatastreamEventProducer.class);

  private Datastream createDatastream() {
    Datastream datastream = new Datastream();
    datastream.setName("dummy datastream");
    datastream.setConnectorType("EspressoConnector");
    datastream.setSource(new DatastreamSource());
    datastream.getSource().setConnectionString("espresso://dummyDB/dummyTable");
    StringMap metadata = new StringMap();
    datastream.setMetadata(metadata);

    DatastreamDestination destination = new DatastreamDestination();
    destination.setConnectionString("dummyDB_dummyTable");
    destination.setPartitions(10);
    datastream.setDestination(destination);

    return datastream;
  }

  private int _eventSeed;

  private DatastreamEventRecord createEventRecord(Datastream datastream, DatastreamTask task) {
    DatastreamEvent event = new DatastreamEvent();
    event.datastream = datastream.getName();
    event.key = ByteBuffer.allocate(1);
    event.payload = ByteBuffer.allocate(1);
    ++_eventSeed;
    return new DatastreamEventRecord(event, 0, "new dummy checkpoint " + String.valueOf(_eventSeed), task);
  }

  private boolean validateCheckpoint(Map<DatastreamTask, String> cpkts, DatastreamTask task, DatastreamEventRecord record) {
    return cpkts.get(task) != null && cpkts.get(task).equals(record.getCheckpoint());
  }

  @Test
  public void testSendWithDatastreamCheckpoint()
      throws InterruptedException, TransportException {
    Datastream datastream = createDatastream();

    DatastreamTask task1 = new DatastreamTaskImpl(datastream);
    DatastreamTask task2 = new DatastreamTaskImpl(datastream);

    List<DatastreamTask> tasks = new ArrayList<>();
    tasks.add(task1);
    tasks.add(task2);

    //ValidatingTransport transport = new ValidatingTransport();
    TransportProvider transport = mock(TransportProvider.class);
    InMemoryCheckpointProvider cpProvider = new InMemoryCheckpointProvider();

    // Checkpoint every 50ms
    Properties config = new Properties();
    config.put(DatastreamEventProducerImpl.CHECKPOINT_PERIOD_MS, "50");

    DatastreamEventProducer producer = new DatastreamEventProducerImpl(tasks, transport, cpProvider, config);

    // No checkpoints for brand new tasks
    Assert.assertEquals(producer.getSafeCheckpoints().size(), 0);

    DatastreamEventRecord record;
    DatastreamTask task;
    DatastreamEventRecord record1 = null;
    DatastreamEventRecord record2 = null;
    for (int i = 0; i < 500; i++) {
      task = i % 3 == 0 ? task1 : task2;
      record = createEventRecord(datastream, task);
      producer.send(record);
      if (task == task1) {
        record1 = record;
      } else {
        record2 = record;
      }
    }

    verify(transport, times(500)).send(any());

    // Allow enough time after the send loop for checkpoint to take place
    Thread.sleep(200);

    // Expect saved checkpoint to match that of the last event
    Map<DatastreamTask, String> checkpoints = cpProvider.getCommitted(tasks);

    // Verify the checkpoints in the provider matches the event records
    Assert.assertTrue(validateCheckpoint(checkpoints, task1, record1));
    Assert.assertTrue(validateCheckpoint(checkpoints, task2, record2));

    // Verify safeCheckpoints match the ones in the checkpointProvider
    Map<DatastreamTask, String> safeCheckpoints = producer.getSafeCheckpoints();
    checkpoints.forEach((k, v) -> Assert.assertEquals(v, safeCheckpoints.get(k)));

    producer.shutdown();

    // Create a new producer
    producer = new DatastreamEventProducerImpl(tasks, transport, cpProvider, config);

    // Expect saved checkpoint to match that of the last event
    Map<DatastreamTask, String> checkpointsNew = producer.getSafeCheckpoints();
    for (DatastreamTask t: checkpoints.keySet()) {
      Assert.assertEquals(checkpoints.get(t), checkpointsNew.get(t));
    }
  }
}
