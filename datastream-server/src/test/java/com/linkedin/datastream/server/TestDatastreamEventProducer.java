package com.linkedin.datastream.server;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.codehaus.jackson.type.TypeReference;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamEvent;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.server.api.schemaregistry.SchemaRegistryProvider;
import com.linkedin.datastream.server.api.transport.TransportException;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.providers.CheckpointProvider;

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

  private DatastreamEventRecord createEventRecord(Datastream datastream, DatastreamTask task, Integer partition) {
    DatastreamEvent event = new DatastreamEvent();
    event.datastream = datastream.getName();
    event.key = ByteBuffer.allocate(1);
    event.payload = ByteBuffer.allocate(1);
    ++_eventSeed;
    return new DatastreamEventRecord(event, partition, "new dummy checkpoint " + String.valueOf(_eventSeed), task);
  }

  private void setup(boolean customCheckpointing) {
    _datastream = createDatastream();

    DatastreamTaskImpl task1 = new DatastreamTaskImpl(_datastream);
    DatastreamTaskImpl task2 = new DatastreamTaskImpl(_datastream);

    List<Integer> partitions1 = new ArrayList<>();
    partitions1.add(1);
    partitions1.add(2);

    List<Integer> partitions2 = new ArrayList<>();
    partitions2.add(3);
    partitions2.add(4);

    task1.setPartitions(partitions1);
    task2.setPartitions(partitions2);

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
  public void testSendWithCustomCheckpoint()
      throws TransportException, InterruptedException {
    setup(true);
    DatastreamTask task;
    Integer partition;
    Random rand = new Random();
    DatastreamEventRecord record;
    for (int i = 0; i < 500; i++) {
      task = i % 3 == 0 ? _tasks.get(0) : _tasks.get(1);
      partition = i % 3 == 0 ? 1 + rand.nextInt(2) : 3 + rand.nextInt(2);
      record = createEventRecord(_datastream, task, partition);
      _producer.send(record);
    }
    final boolean[] isCommitCalled = {false};

    verify(_transport, times(500)).send(any());
    doAnswer(invocation -> isCommitCalled[0] = true).when(_cpProvider).commit(anyMap());

    // Ensure that commit is not called even after 1 second.
    Thread.sleep(1000);
    Assert.assertTrue(!isCommitCalled[0]);
  }

  private boolean validateCheckpoint(CheckpointProvider provider,
                                     List<DatastreamTask> tasks,
                                     Map<DatastreamTask, Map<Integer, String>> taskCpMap) {
    Map<DatastreamTask, String> checkpoints = provider.getCommitted(tasks);
    for (DatastreamTask task : tasks) {
      String cpString = checkpoints.getOrDefault(task, null);
      if (cpString == null) { // not ready
        return false;
      }
      TypeReference typeRef = new TypeReference<HashMap<Integer, String>>() {};
      Map<Integer, String> cpMap = JsonUtils.fromJson(cpString, typeRef);
      if (!cpMap.equals(taskCpMap.get(task))) {
        return false;
      }
    }
    return true;
  }

  @Test
  public void testSendWithDatastreamCheckpoint()
      throws InterruptedException, TransportException {

    setup(false);

    Map<DatastreamTask, Map<Integer, String>> taskCpMap = new HashMap<>();

    DatastreamTask task;
    Integer partition;
    Random rand = new Random();
    DatastreamEventRecord record;
    for (int i = 0; i < 500; i++) {
      task = i % 3 == 0 ? _tasks.get(0) : _tasks.get(1);
      partition = i % 3 == 0 ? 1 + rand.nextInt(2) : 3 + rand.nextInt(2);
      record = createEventRecord(_datastream, task, partition);
      _producer.send(record);

      Map<Integer, String> cpMap = taskCpMap.getOrDefault(task, new HashMap<>());
      cpMap.put(partition, record.getCheckpoint());
      taskCpMap.put(task, cpMap);
    }

    verify(_transport, times(500)).send(any());

    // Verify all safe checkpoints from producer match the last event
    Assert.assertTrue(PollUtils.poll(() -> validateCheckpoint(_cpProvider, _tasks, taskCpMap), 50, 200));

    // Verify safeCheckpoints match the ones in the checkpointProvider
    Assert.assertEquals(_producer.getSafeCheckpoints(), taskCpMap);

    _producer.shutdown();

    // Create a new producer
    _producer = new DatastreamEventProducerImpl(_tasks, _transport, _schemaRegistryProvider, _cpProvider, _config, false);

    // Expect saved checkpoint to match that of the last event
    Map<DatastreamTask, Map<Integer, String>> checkpointsNew = _producer.getSafeCheckpoints();
    Assert.assertEquals(checkpointsNew, taskCpMap);
  }
}
