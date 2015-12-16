package com.linkedin.datastream.server;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamEvent;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.server.api.schemaregistry.SchemaRegistryProvider;
import com.linkedin.datastream.server.api.transport.TransportException;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.providers.CheckpointProvider;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.doAnswer;
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
  private ArrayList<DatastreamTask> _tasks;
  private DatastreamEventProducerImpl _producer;
  private Datastream _datastream;
  private TransportProvider _transport;
  private CheckpointProvider _cpProvider;
  private SchemaRegistryProvider _schemaRegistryProvider;
  private Properties _config;

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
  public void testSendWithCustomCheckpoint()
      throws TransportException, InterruptedException {
    setup(true);
    DatastreamEventRecord record;
    DatastreamTask task;
    for (int i = 0; i < 500; i++) {
      task = i % 3 == 0 ? _tasks.get(0) : _tasks.get(1);
      record = createEventRecord(_datastream, task);
      _producer.send(record);
    }
    final boolean[] isCommitCalled = {false};

    verify(_transport, times(500)).send(any());
    doAnswer(invocation -> isCommitCalled[0] = true).when(_cpProvider).commit(anyMap());

    // Ensure that commit is not called even after 1 second.
    Thread.sleep(1000);
    Assert.assertTrue(!isCommitCalled[0]);
  }



  private void setup(boolean customCheckpointing) {
    _datastream = createDatastream();

    DatastreamTask task1 = new DatastreamTaskImpl(_datastream);
    DatastreamTask task2 = new DatastreamTaskImpl(_datastream);

    _tasks = new ArrayList<>();
    _tasks.add(task1);
    _tasks.add(task2);

    //ValidatingTransport transport = new ValidatingTransport();
    _transport = mock(TransportProvider.class);
    _schemaRegistryProvider = mock(SchemaRegistryProvider.class);
    if(!customCheckpointing) {
      _cpProvider = new InMemoryCheckpointProvider();
    } else {
      _cpProvider = mock(CheckpointProvider.class);
    }

    // Checkpoint every 50ms
    _config = new Properties();
    _config.put(DatastreamEventProducerImpl.CHECKPOINT_PERIOD_MS, "50");

    _producer = new DatastreamEventProducerImpl(_tasks, _transport, _schemaRegistryProvider, _cpProvider, _config, customCheckpointing);
  }

  @Test
  public void testSendWithDatastreamCheckpoint()
      throws InterruptedException, TransportException {

    setup(false);
    // No checkpoints for brand new tasks
    Assert.assertEquals(_producer.getSafeCheckpoints().size(), 0);

    DatastreamEventRecord record;
    DatastreamTask task;
    DatastreamEventRecord record1 = null;
    DatastreamEventRecord record2 = null;
    for (int i = 0; i < 500; i++) {
      task = i % 3 == 0 ? _tasks.get(0) : _tasks.get(1);
      record = createEventRecord(_datastream, task);
      _producer.send(record);
      if (task == _tasks.get(0)) {
        record1 = record;
      } else {
        record2 = record;
      }
    }

    verify(_transport, times(500)).send(any());
    InMemoryCheckpointProvider cpProvider = (InMemoryCheckpointProvider) _cpProvider;

    // Allow enough time after the send loop for checkpoint to take place
    Thread.sleep(200);

    // Expect saved checkpoint to match that of the last event
    Map<DatastreamTask, String> checkpoints = _cpProvider.getCommitted(_tasks);

    // Verify the checkpoints in the provider matches the event records
    Assert.assertTrue(validateCheckpoint(checkpoints, _tasks.get(0), record1));
    Assert.assertTrue(validateCheckpoint(checkpoints, _tasks.get(1), record2));

    // Verify safeCheckpoints match the ones in the checkpointProvider
    Map<DatastreamTask, String> safeCheckpoints = _producer.getSafeCheckpoints();
    checkpoints.forEach((k, v) -> Assert.assertEquals(v, safeCheckpoints.get(k)));

    _producer.shutdown();

    // Create a new producer
    _producer = new DatastreamEventProducerImpl(_tasks, _transport, _schemaRegistryProvider, _cpProvider, _config, false);

    // Expect saved checkpoint to match that of the last event
    Map<DatastreamTask, String> checkpointsNew = _producer.getSafeCheckpoints();
    for (DatastreamTask t: checkpoints.keySet()) {
      Assert.assertEquals(checkpoints.get(t), checkpointsNew.get(t));
    }
  }
}
