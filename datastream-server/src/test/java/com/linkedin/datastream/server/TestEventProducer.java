package com.linkedin.datastream.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.codehaus.jackson.type.TypeReference;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamEvent;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.server.api.transport.TransportException;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.providers.CheckpointProvider;
import com.linkedin.datastream.testutil.InMemoryCheckpointProvider;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;


public class TestEventProducer {
  private ArrayList<DatastreamTask> _tasks;
  private EventProducer _producer;
  private Datastream _datastream;
  private TransportProvider _transport;
  private CheckpointProvider _cpProvider;
  private Properties _config;
  private AtomicInteger _inSend = new AtomicInteger(0);
  private AtomicBoolean _inFlush = new AtomicBoolean(false);

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

  private DatastreamEventRecord createEventRecord(Integer partition) {
    DatastreamEvent event = new DatastreamEvent();
    event.key = null;
    event.payload = null;
    event.previous_payload = null;
    ++_eventSeed;
    return new DatastreamEventRecord(event, partition, "new dummy checkpoint " + String.valueOf(_eventSeed));
  }

  private void setup(boolean customCheckpointing) {
    setup(customCheckpointing, false);
  }

  private void setup(boolean customCheckpointing, boolean checkRace) {
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

    _transport = mock(TransportProvider.class);

    // Verify transport.send() and transport.flush() is never called simultaneously
    if (checkRace) {
      try {
        doAnswer((invocation) -> {
          Assert.assertFalse(_inFlush.get());
          _inSend.incrementAndGet();
          try {
            Thread.sleep(1); // mimic send delay
          } catch (InterruptedException e) {
            Assert.fail();
          }
          _inSend.decrementAndGet();
          return null;
        }).when(_transport).send(anyString(), anyObject());

        doAnswer((invocation) -> {
          Assert.assertEquals(_inSend.get(), 0);
          _inFlush.set(true);
          try {
            Thread.sleep(3); // mimic send delay
          } catch (InterruptedException e) {
            Assert.fail();
          }
          _inFlush.set(false);
          return null;
        }).when(_transport).flush();
      } catch (TransportException e) {
        Assert.fail(); // this is impossible
      }
    }

    if (!customCheckpointing) {
      _cpProvider = new InMemoryCheckpointProvider();
    } else {
      _cpProvider = mock(CheckpointProvider.class);
    }

    // Checkpoint every 50ms
    _config = new Properties();
    _config.put(EventProducer.CHECKPOINT_PERIOD_MS, "50");

    _producer = new EventProducer(_tasks, _transport, _cpProvider, _config, customCheckpointing);
  }

  @Test
  public void testSendWithCustomCheckpoint() throws DatastreamException, TransportException, InterruptedException {
    setup(true);
    DatastreamTask task;
    Integer partition;
    Random rand = new Random();
    DatastreamEventRecord record;
    for (int i = 0; i < 500; i++) {
      task = i % 3 == 0 ? _tasks.get(0) : _tasks.get(1);
      partition = i % 3 == 0 ? 1 + rand.nextInt(2) : 3 + rand.nextInt(2);
      record = createEventRecord(partition);
      _producer.send(task, record);
    }
    final boolean[] isCommitCalled = { false };

    verify(_transport, times(500)).send(any(), any());
    doAnswer(invocation -> isCommitCalled[0] = true).when(_cpProvider).commit(anyMap());

    // Ensure that commit is not called even after 1 second.
    Thread.sleep(1000);
    Assert.assertTrue(!isCommitCalled[0]);
  }

  private boolean validateCheckpoint(CheckpointProvider provider, List<DatastreamTask> tasks,
      Map<DatastreamTask, Map<Integer, String>> taskCpMap) {
    Map<DatastreamTask, String> checkpoints = provider.getCommitted(tasks);
    for (DatastreamTask task : tasks) {
      String cpString = checkpoints.getOrDefault(task, null);
      if (cpString == null) { // not ready
        return false;
      }
      TypeReference typeRef = new TypeReference<HashMap<Integer, String>>() {
      };
      Map<Integer, String> cpMap = JsonUtils.fromJson(cpString, typeRef);
      if (!cpMap.equals(taskCpMap.get(task))) {
        return false;
      }
    }
    return true;
  }

  @Test
  public void testSendWithDatastreamCheckpoint() throws DatastreamException, InterruptedException, TransportException {

    setup(false);

    Map<DatastreamTask, Map<Integer, String>> taskCpMap = new HashMap<>();

    DatastreamTask task;
    Integer partition;
    Random rand = new Random();
    DatastreamEventRecord record;
    for (int i = 0; i < 500; i++) {
      task = i % 3 == 0 ? _tasks.get(0) : _tasks.get(1);
      partition = i % 3 == 0 ? 1 + rand.nextInt(2) : 3 + rand.nextInt(2);
      record = createEventRecord(partition);
      _producer.send(task, record);

      Map<Integer, String> cpMap = taskCpMap.getOrDefault(task, new HashMap<>());
      cpMap.put(partition, record.getCheckpoint());
      taskCpMap.put(task, cpMap);
    }

    verify(_transport, times(500)).send(any(), any());

    // Verify all safe checkpoints from producer match the last event
    Assert.assertTrue(PollUtils.poll(() -> validateCheckpoint(_cpProvider, _tasks, taskCpMap), 50, 200));

    // Verify safeCheckpoints match the ones in the checkpointProvider
    Assert.assertEquals(_producer.getSafeCheckpoints(), taskCpMap);

    _producer.shutdown();

    // Create a new producer
    _producer =
        new EventProducer(_tasks, _transport, _cpProvider, _config, false);

    // Expect saved checkpoint to match that of the last event
    Map<DatastreamTask, Map<Integer, String>> checkpointsNew = _producer.getSafeCheckpoints();
    Assert.assertEquals(checkpointsNew, taskCpMap);
  }

  private void verifyCheckpoints() {
    int size = _tasks.size();
    Assert.assertEquals(_producer.getSafeCheckpoints().size(), size);
    _tasks.forEach(t -> {
      Assert.assertNotNull(_producer.getSafeCheckpoints().get(t), t.toString());
      Assert.assertEquals(_producer.getSafeCheckpoints().get(t).keySet(), t.getPartitions(), t.toString());
    });
  }

  /**
   * Helper for tests exercising updateTasks
   *
   * 1. add a new task (task3)
   * 2. remove a task (task3)
   * 3. add task3 and remove task2
   *
   * Validate the checkpoint map after each step.
   */
  private void callUpdateTasksAndVerify(int partition) {
    List<Integer> partitions = new ArrayList<>();
    partitions.add(partition);
    partitions.add(partition + 1);
    DatastreamTaskImpl task3 = new DatastreamTaskImpl(_datastream);
    task3.setPartitions(partitions);
    _tasks.add(task3);
    _producer.updateTasks(_tasks);
    verifyCheckpoints();

    // Remove a task
    _tasks.remove(2);
    _producer.updateTasks(_tasks);
    verifyCheckpoints();

    // Add and remove a task
    _tasks.remove(1);
    _tasks.add(task3);
    _producer.updateTasks(_tasks);
    verifyCheckpoints();
  }

  /**
   * Basic test case of update tasks
   */
  @Test
  public void testUpdateTasks() {
    setup(false);

    verifyCheckpoints();
    callUpdateTasksAndVerify(5);
  }

  /**
   * Using three threads doing repeated send/flush/updateTasks and check for race conditions.
   */
  @Test
  public void testSendFlushUpdateTaskStress() {
    setup(false, true);

    // Mimic a connector constantly calls send/flush
    final boolean stopHandler[] = { false };
    Thread sender = new Thread(() -> {
      DatastreamTask task = _tasks.get(0);
      while (!stopHandler[0]) {
        try {
          Thread.sleep(5);
        } catch (InterruptedException e) {
          break;
        }

        _producer.send(task, createEventRecord(1));
      }
    });
    sender.start();

    Thread flusher = new Thread(() -> {
      DatastreamTask task = _tasks.get(0);
      while (!stopHandler[0]) {
        try {
          Thread.sleep(5);
        } catch (InterruptedException e) {
          break;
        }

        _producer.flush();
      }
    });
    flusher.start();

    // Repeatedly update the tasks and check for corruption
    int iterations = 100;
    for (int i = 0, step = 0; i < iterations; i++, step += 2) {
      callUpdateTasksAndVerify(step + 100);
      try {
        Thread.sleep(2);
      } catch (InterruptedException e) {
        Assert.fail();
      }
    }

    stopHandler[0] = true;
    Assert.assertTrue(PollUtils.poll(() -> !flusher.isAlive(), 50, 2000));

    // Remove all tasks
    _tasks.clear();
    _producer.updateTasks(_tasks);
    verifyCheckpoints();

  }
}
