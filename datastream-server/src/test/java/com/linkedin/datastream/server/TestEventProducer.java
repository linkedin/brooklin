package com.linkedin.datastream.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamEvent;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.server.api.transport.DatastreamRecordMetadata;
import com.linkedin.datastream.server.api.transport.SendCallback;
import com.linkedin.datastream.server.api.transport.TransportException;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.providers.CheckpointProvider;
import com.linkedin.datastream.testutil.InMemoryCheckpointProvider;


public class TestEventProducer {
  private static final Logger LOG = LoggerFactory.getLogger(TestEventProducer.class);

  private ArrayList<DatastreamTask> _tasks;
  private EventProducer _producer;
  private Datastream _datastream;
  private TransportProvider _transport;
  private CheckpointProvider _cpProvider;
  private Properties _config;
  private Random _random = new Random();

  private Datastream createDatastream() {
    Datastream datastream = new Datastream();
    datastream.setName("dummy datastream");
    datastream.setConnectorName("EspressoConnector");
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

  private DatastreamProducerRecord createEventRecord(Integer partition) {
    DatastreamEvent event = new DatastreamEvent();
    event.key = null;
    event.payload = null;
    event.previous_payload = null;
    ++_eventSeed;
    DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
    builder.addEvent(event);
    builder.setPartition(partition);
    builder.setSourceCheckpoint("new dummy checkpoint " + String.valueOf(_eventSeed));
    builder.setEventsTimestamp(System.currentTimeMillis());
    return builder.build();
  }

  private void setup(boolean customCheckpointing)
      throws TransportException {
    setup(customCheckpointing, false, null);
  }

  private void setup(boolean customCheckpointing, boolean throwExceptionOnSend,
      Consumer<EventProducer> onUnrecoverableError)
      throws TransportException {
    _datastream = createDatastream();

    // Start with two tasks covering partition [0,1] and [2,3]
    DatastreamTaskImpl task0 = new DatastreamTaskImpl(_datastream);
    DatastreamTaskImpl task1 = new DatastreamTaskImpl(_datastream);

    List<Integer> partitions1 = new ArrayList<>();
    partitions1.add(0);
    partitions1.add(1);

    List<Integer> partitions2 = new ArrayList<>();
    partitions2.add(2);
    partitions2.add(3);

    task0.setPartitions(partitions1);
    task1.setPartitions(partitions2);

    _tasks = new ArrayList<>();
    _tasks.add(task0);
    _tasks.add(task1);

    _transport = mock(TransportProvider.class);

    doAnswer(invocation -> {
      Object[] args = invocation.getArguments();
      String topic = (String) args[0];
      DatastreamProducerRecord record = (DatastreamProducerRecord) args[1];
      SendCallback callback = (SendCallback) args[2];
      if (callback != null) {
        Exception exception = null;
        if (throwExceptionOnSend) {
          exception = new DatastreamRuntimeException();
        }
        callback.onCompletion(new DatastreamRecordMetadata(record.getCheckpoint(), topic, record.getPartition().get()),
            exception);
      }
      return null;
    }).when(_transport).send(anyString(), anyObject(), anyObject());

    if (!customCheckpointing) {
      _cpProvider = new InMemoryCheckpointProvider();
    } else {
      _cpProvider = mock(CheckpointProvider.class);
    }

    // Checkpoint every 50ms
    _config = new Properties();
    _config.put(EventProducer.CHECKPOINT_PERIOD_MS, "50");

    _producer = new EventProducer(_transport, _cpProvider, _config, customCheckpointing, onUnrecoverableError, 0);
    _tasks.forEach(t -> _producer.assignTask(t));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testSendWithCustomCheckpoint()
      throws DatastreamException, TransportException, InterruptedException {
    setup(true);
    DatastreamTask task;
    Integer partition;
    Random rand = new Random();
    DatastreamProducerRecord record;
    for (int i = 0; i < 500; i++) {
      task = i % 3 == 0 ? _tasks.get(0) : _tasks.get(1);
      partition = i % 3 == 0 ? 1 + rand.nextInt(2) : 3 + rand.nextInt(2);
      record = createEventRecord(partition);
      _producer.send(task, record, null);
    }
    final boolean[] isCommitCalled = {false};

    verify(_transport, times(500)).send(any(), any(), anyObject());
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
        LOG.info("Committed checkpoints is null");
        return false;
      }
      TypeReference<HashMap<Integer, String>> typeRef = new TypeReference<HashMap<Integer, String>>() {
      };
      Map<Integer, String> cpMap = JsonUtils.fromJson(cpString, typeRef);

      LOG.info(String.format("Committed checkpoints %s, Expected checkpoints %s", cpMap, taskCpMap));
      if (!cpMap.equals(taskCpMap.get(task))) {
        return false;
      }
    }
    return true;
  }

  @Test
  public void testProducerUnrecoverableErrorCalled()
      throws TransportException {

    List<EventProducer> producersWithUnrecoverableError = new ArrayList<>();
    setup(false, true, producersWithUnrecoverableError::add);
    List<Exception> exceptions = new ArrayList<>();
    List<DatastreamRecordMetadata> producedMetadata = new ArrayList<>();
    DatastreamProducerRecord record = createEventRecord(0);
    _producer.send(_tasks.get(0), record, (metadata, exception) -> {

      producedMetadata.add(metadata);
      if (exception != null) {
        exceptions.add(exception);
      }
    });

    Assert.assertEquals(producersWithUnrecoverableError.size(), 1);
    Assert.assertEquals(producersWithUnrecoverableError.get(0), _producer);
    Assert.assertEquals(exceptions.size(), 1);
    Assert.assertEquals(producedMetadata.size(), 1);
    Assert.assertEquals(producedMetadata.get(0).getCheckpoint(), record.getCheckpoint());
    Assert.assertEquals(producedMetadata.get(0).getPartition(), 0);
  }

  @Test
  public void testProducerCallsCallback()
      throws TransportException {
    setup(false);
    List<DatastreamRecordMetadata> producedMetadata = new ArrayList<>();
    DatastreamProducerRecord record = createEventRecord(0);
    _producer.send(_tasks.get(0), record, (m, e) -> producedMetadata.add(m));
    Assert.assertEquals(producedMetadata.size(), 1);
    Assert.assertEquals(producedMetadata.get(0).getCheckpoint(), record.getCheckpoint());
    Assert.assertEquals(producedMetadata.get(0).getPartition(), 0);
  }

  @Test
  public void testProducerCallsCallbackWithException()
      throws TransportException {
    setup(false, true, null);
    List<Exception> exceptions = new ArrayList<>();
    List<DatastreamRecordMetadata> producedMetadata = new ArrayList<>();
    DatastreamProducerRecord record = createEventRecord(0);
    _producer.send(_tasks.get(0), record, (metadata, exception) -> {

      producedMetadata.add(metadata);
      if (exception != null) {
        exceptions.add(exception);
      }
    });

    Assert.assertEquals(exceptions.size(), 1);
    Assert.assertEquals(producedMetadata.size(), 1);
    Assert.assertEquals(producedMetadata.get(0).getCheckpoint(), record.getCheckpoint());
    Assert.assertEquals(producedMetadata.get(0).getPartition(), 0);
  }

  @Test
  public void testSendWithDatastreamCheckpoint()
      throws DatastreamException, InterruptedException, TransportException {

    setup(false);
    Map<DatastreamTask, Map<Integer, String>> taskCpMap = new HashMap<>();

    DatastreamTask task;
    Integer partition;
    Random rand = new Random();
    DatastreamProducerRecord record;
    for (int i = 0; i < 500; i++) {
      // choose task0 for iteration of multiple of 3, otherwise task1
      task = i % 3 == 0 ? _tasks.get(0) : _tasks.get(1);
      int partitions = task.getPartitions().size();
      partition = task.getPartitions().get(rand.nextInt(partitions));
      record = createEventRecord(partition);
      _producer.send(task, record, null);

      Map<Integer, String> cpMap = taskCpMap.getOrDefault(task, new HashMap<>());
      cpMap.put(partition, record.getCheckpoint());
      taskCpMap.put(task, cpMap);
    }

    verify(_transport, times(500)).send(any(), any(), any());

    // Verify all safe checkpoints from producer match the last event
    Assert.assertTrue(PollUtils.poll(() -> validateCheckpoint(_cpProvider, _tasks, taskCpMap), 100, 5000));

    // Verify safeCheckpoints match the ones in the checkpointProvider
    Assert.assertEquals(_producer.getSafeCheckpoints(), taskCpMap);

    _producer.shutdown();

    // Create a new producer
    _producer = new EventProducer(_transport, _cpProvider, _config, false, null, 0);

    _tasks.forEach(t -> _producer.assignTask(t));
    // Expect saved checkpoint to match that of the last event
    Map<DatastreamTask, Map<Integer, String>> checkpointsNew = _producer.getSafeCheckpoints();
    Assert.assertEquals(checkpointsNew, taskCpMap);
  }

  private void verifyCheckpoints() {
    int size = _tasks.size();
    // # of checkpoint entries must equal # of tasks
    Assert.assertEquals(_producer.getSafeCheckpoints().size(), size);
    _tasks.forEach(t -> {
      // There must be a checkpoint for an assigned task
      Assert.assertNotNull(_producer.getSafeCheckpoints().get(t));
      // Checkpoint map of a task must have the same number of entries as number of partitions in the task
      Assert.assertEquals(_producer.getSafeCheckpoints().get(t).keySet(), t.getPartitions());
    });
  }

  /**
   * Helper for tests exercising assign/unassignTasks
   *
   * 1. assign a new task (task2)
   * 2. unassign a task (task2)
   * 3. unassign task2
   * 4. assign task3
   *
   * Validate the checkpoint map after each step.
   * Invariant:
   *  entry: 2 tasks assigned
   *  exit: 2 tasks assigned
   *  task0 is always assigned (for sender)
   */
  private void callUpdateTasksAndVerify(int partition) {
    List<Integer> partitions = new ArrayList<>();
    partitions.add(partition);
    partitions.add(partition + 1);
    DatastreamTaskImpl task2 = new DatastreamTaskImpl(_datastream);
    task2.setPartitions(partitions);
    _tasks.add(task2);
    _producer.assignTask(task2);
    verifyCheckpoints();

    // Unassign task2
    _producer.unassignTask(_tasks.get(2));
    _tasks.remove(2);
    verifyCheckpoints();

    // unassign task1
    _producer.unassignTask(_tasks.get(1));
    _tasks.remove(1);

    // assign task2 back
    _producer.assignTask(task2);
    _tasks.add(task2);

    verifyCheckpoints();
  }

  /**
   * Basic test case of assign/unassign tasks
   */
  @Test
  public void testUpdateTasks()
      throws TransportException {
    setup(false);

    verifyCheckpoints();
    callUpdateTasksAndVerify(5);
  }

  private void randomSleep(int limit) {
    try {
      Thread.sleep(1 + _random.nextInt(limit));
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted"); // Force abort with runtime exception if interrupted
    }
  }

  /**
   * Using three threads doing repeated send/flush/(un)assignTasks and check for race conditions.
   * The aim for the test is to stress the synchronization needed among onSendCallback(), flush(), and
   * assign/unassignTasks in EventProducer. The transport provider is also mocked to ensure send()
   * and flush() are never interleaved.
   *
   * Test logic:
   *  1) create two threads as the sender and flusher that repeatedly
   *    - sender: send events with producer.send()
   *    - flusher: flush events with producer.flush()
   *  2) use the main thread to call assign/unassignTask repeatedly
   *  3) add random sleeps between back-to-back operations
   *  4) start both sender/flusher and wait
   *  5) perform 100 iterations of assign/unassignTasks
   *  6) signal stop to both send and flusher
   *
   *  In each iteration, the validity of checkpoints maintained by the producer are validated
   */
  @Test
  public void testSendFlushUpdateTaskStress()
      throws TransportException {
    setup(false, false, null);

    AtomicBoolean stopFlag = new AtomicBoolean();

    // Mimic a connector constantly calls send/flush
    Thread sender = new Thread(() -> {
      DatastreamTask task = _tasks.get(0); // task0 always exists
      while (!stopFlag.get()) {
        randomSleep(5);

        // Send to the first partition of task0
        int partition = task.getPartitions().get(0);

        // Keep task0 intact to avoid the need to synchronize
        // sender with task updater (main test thread) since
        // they access the same _tasks array.
        _producer.send(task, createEventRecord(partition), null);
      }
    });

    LOG.info("Starting sender thread.");
    sender.setUncaughtExceptionHandler((a1, a2) -> Assert.fail("Sender failed"));
    sender.start();

    Thread flusher = new Thread(() -> {
      while (!stopFlag.get()) {
        randomSleep(5);
        _producer.flushAndCheckpoint();
      }
    });

    LOG.info("Starting flusher thread.");
    sender.setUncaughtExceptionHandler((a1, a2) -> Assert.fail("Flusher failed"));
    flusher.start();

    LOG.info("Starting assign/unassignTask loop.");
    int iterations = 100;
    // Each iteration we use a different partition number to avoid
    // overlapping with task0 which is always assigned with p0 and p1
    for (int i = 0, partition = 0; i < iterations; i++, partition += 2) {
      callUpdateTasksAndVerify(partition + 100);
      randomSleep(2);
    }

    // Signal stop to sender/flusher
    stopFlag.set(true);

    // Ensure both sender and flusher have exited completely
    Assert.assertTrue(PollUtils.poll(() -> !flusher.isAlive() && !sender.isAlive(), 50, 2000));

    // Unassigned all tasks
    _tasks.forEach(t -> _producer.unassignTask(t));
    _tasks.clear();

    verifyCheckpoints();
  }
}
