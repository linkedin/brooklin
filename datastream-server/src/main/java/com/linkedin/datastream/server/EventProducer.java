package com.linkedin.datastream.server;

import com.linkedin.datastream.common.DatastreamEvent;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.api.transport.TransportException;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.providers.CheckpointProvider;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;


/**
* EventProducer class uses the transport to send events and handles save/restore checkpoints
* automatically if {@link EventProducer.CheckpointPolicy#DATASTREAM} is specified.
* Otherwise, it exposes the safe checkpoints which are guaranteed to have been flushed.
*/
public class EventProducer {
  private static final Logger LOG = LoggerFactory.getLogger(DatastreamEventProducerImpl.class);

  /**
   * Policy for checkpoint handling
   */
  enum CheckpointPolicy {
    DATASTREAM,
    CUSTOM
  }

  public static final String INVALID_CHECKPOINT = "";
  public static final String CHECKPOINT_PERIOD_MS = "checkpointPeriodMs";
  public static final Integer DEFAULT_CHECKPOINT_PERIOD_MS = 1000;
  public static final Integer SHUTDOWN_POLL_MS = 1000;
  public static final Integer SHUTDOWN_POLL_PERIOD_MS = 50;

  // List of tasks the producer is responsible for
  private final Set<DatastreamTask> _tasks;

  private final TransportProvider _transportProvider;
  private final CheckpointProvider _checkpointProvider;
  private final CheckpointPolicy _checkpointPolicy;

  // This stores the latest "dirty" checkpoint that we received
  // from send() call but haven't been flushed on the transport.
  // One one checkpoint is needed since connector only need to
  // fulfill all the tasks by only producing any one of them,
  // hence producer just need to store one dirty checkpoint and
  // update all tasks in _safeCheckpoints with this.
  private Map<DatastreamTask, Map<Integer, String>> _latestCheckpoints;

  // This stores the checkpoints that have been recently flushed such that
  // are safe to be committed to the source consumption tracking system.
  private Map<DatastreamTask, Map<Integer, String>> _safeCheckpoints;

  // Helper for periodical flush/checkpoint operations
  private final CheckpointHandler _checkpointHandler;

  // Flag to quickly tell if there are pending events
  private volatile boolean _pendingCheckpoint = false;

  private volatile boolean _shutdownCompleted = false;
  private volatile boolean _shutdownRequested = false;

  // This read-write lock synchronizes send/flush/updateTasks
  //  a. send is reader because multiple send are safe to run concurrently
  //  b. flush/updateTasks is writer because they need to run in critical
  //     section as multiple structures are modified
  private ReentrantReadWriteLock _sendFlushLock;

  /**
   * Flush transport periodically and save checkpoints.
   */
  class CheckpointHandler implements Runnable {
    private final Long _periodMs;
    private final ScheduledExecutorService _executor;

    public CheckpointHandler(Properties config) {
      VerifiableProperties props = new VerifiableProperties(config);
      _periodMs = props.getLong(CHECKPOINT_PERIOD_MS, DEFAULT_CHECKPOINT_PERIOD_MS);
      _executor = new ScheduledThreadPoolExecutor(1);
      _executor.scheduleAtFixedRate(this, 0, _periodMs, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
      LOG.info(String.format("Shutdown requested, tasks = [%s].", _tasks));

      // Initiate shutdown
      _executor.shutdown();

      // Poll for the thread to actually exit
      PollUtils.poll(() -> _executor.isTerminated(), SHUTDOWN_POLL_MS, SHUTDOWN_POLL_PERIOD_MS);

      // Final flush
      flushAndCheckpoint();

      LOG.info(String.format("Shutdown finished, tasks = [%s].", _tasks));
    }

    private void flushAndCheckpoint() {
      try {
        flush();
        LOG.debug(String.format("Checkpoint handler exited, tasks = [%s].", _tasks));
      } catch (Throwable t) {
        // Since have have handled all exceptions here, there would be no exceptions leaked
        // to the executor, which will reschedules as usual so no explicit retry is needed.
        LOG.error(String.format("Checkpoint failed, will retry after %d ms, reason = %s", _periodMs, t.getMessage()), t);
      }
    }

    @Override
    public void run() {
      flushAndCheckpoint();
    }
  }

  /**
   * Construct an EventProducer instance.
   * @param tasks list of tasks which have the same destination
   * @param transportProvider event transport
   * @param checkpointProvider checkpoint store
   * @param config global config
   * @param customCheckpointing decides whether Producer should use custom checkpointing or the datastream server
   *                            provided checkpointing.
   */
  public EventProducer(List<DatastreamTask> tasks, TransportProvider transportProvider,
      CheckpointProvider checkpointProvider, Properties config, boolean customCheckpointing) {
    Validate.notNull(tasks, "null tasks");
    Validate.notNull(transportProvider, "null transport provider");
    Validate.notNull(checkpointProvider, "null checkpoint provider");
    Validate.notNull(config, "null config");

    // Using a fair lock to prevent flush getting starved by send
    _sendFlushLock = new ReentrantReadWriteLock(/* fairness */ true);

    _transportProvider = transportProvider;
    _checkpointProvider = checkpointProvider;
    _checkpointPolicy = customCheckpointing ? CheckpointPolicy.CUSTOM : CheckpointPolicy.DATASTREAM;
    _tasks = new HashSet<>();
    _safeCheckpoints = new HashMap<>();

    // Prepare checkpoints for all tasks
    updateTasks(tasks);

    // Start checkpoint handler
    _checkpointHandler = new CheckpointHandler(config);

    LOG.info("Created event producer, tasks: " + tasks + ", safe checkpoints: " + _safeCheckpoints);
  }

  /**
   * This method should be called with sendFluchLock.writeLock acquired.
   */
  private void loadCheckpoints(List<DatastreamTask> tasks) {
    Map<DatastreamTask, String> committed = _checkpointProvider.getCommitted(tasks);

    // Instruct jackson to convert string keys to integer
    TypeReference<HashMap<Integer, String>> typeRef = new TypeReference<HashMap<Integer, String>>() {
    };

    // Load checkpoints only for specified task list
    for (DatastreamTask task : tasks) {
      String cpString = committed.get(task);
      if (!StringUtils.isBlank(cpString)) {
        // Deserialize checkpoints from JSON
        try {
          _safeCheckpoints.put(task, JsonUtils.fromJson(cpString, typeRef));
        } catch (Exception e) {
          throw new RuntimeException("Failed to load checkpoints from json: " + cpString + ", task=" + task, e);
        }
      } else {
        // Brand new task without checkpoints
        Map<Integer, String> cpMap = new HashMap<>();
        task.getPartitions().forEach(partition -> cpMap.put(partition, INVALID_CHECKPOINT));
        _safeCheckpoints.put(task, cpMap);
      }
    }
  }

  private void validateAndNormalizeEventRecord(DatastreamEventRecord record) {
    Validate.notNull(record, "null event record.");
    Validate.notNull(record.getEvents(), "null event payload.");
    Validate.notNull(record.getCheckpoint(), "null event checkpoint.");

    for (DatastreamEvent event : record.getEvents()) {
      if (event.metadata == null) {
        event.metadata = new HashMap<>();
      }

      event.key = event.key == null ? ByteBuffer.allocate(0) : event.key;
      event.payload = event.payload == null ? ByteBuffer.allocate(0) : event.payload;
      event.previous_payload = event.previous_payload == null ? ByteBuffer.allocate(0) : event.previous_payload;
    }
  }

  /**
   * For a new task assignment, this method should be called to update the internal
   * checkpoint structures to accommodate for newly assigned and unassigned tasks.
   *
   * @param assignment current task assignment
   */
  public void updateTasks(Collection<DatastreamTask> assignment) {
    Validate.notNull(assignment, "null assigned tasks");

    // Calculate newly assigned and unassigned tasks
    Set<DatastreamTask> assigned = new HashSet<>(assignment);
    Set<DatastreamTask> unassigned = new HashSet<>(_tasks);
    unassigned.removeAll(assignment);
    assigned.removeAll(_tasks);
    if (assigned.size() == 0 && unassigned.size() == 0) { // no changes
      return;
    }

    // Validate newly assigned tasks
    //  a. all tasks pointing to the same destination
    //  b. no overlapping partitions among all tasks
    DatastreamTask task0 = _tasks.size() > 0 ? _tasks.iterator().next() : assigned.iterator().next();
    for (DatastreamTask task : assigned) {
      // Ensure all tasks are for the same destination
      task.getDatastreamDestination().equals(task0.getDatastreamDestination());

      // DatastreamTask should include at least one partition.
      // This should be ensured by DatastreamTaskImpl's ctor.
      Validate.notEmpty(task.getPartitions(), "null or empty partitions in: " + task);
    }

    try {
      // Write lock is required
      _sendFlushLock.writeLock().lock();

      // Flush any outstanding events
      flush();

      // Remove unassigned tasks and their checkpoints
      unassigned.forEach(_safeCheckpoints::remove);
      _tasks.removeAll(unassigned);

      // Add newly assigned tasks and populate checkpoints
      loadCheckpoints(new ArrayList<>(assigned));
      _tasks.addAll(assigned);

      // Collect all partitions
      Set<Integer> knownPartitions = new HashSet<>();
      _tasks.forEach(t -> {
        t.getPartitions().forEach(p -> Validate.isTrue(!knownPartitions.contains(p), "duplicate partition: " + p));
        knownPartitions.addAll(t.getPartitions());
      });

      _latestCheckpoints = new HashMap<>(_safeCheckpoints);
    } finally {
      _sendFlushLock.writeLock().unlock();
    }
  }

  /**
   * Send the event onto the underlying transport.
   *
   * @param record DatastreamEvent envelope
   */
  public void send(DatastreamTask task, DatastreamEventRecord record) {
    // Prevent sending if we have been shutdown
    if (_shutdownRequested) {
      throw new IllegalStateException("send() is not allowed on a shutdown producer");
    }

    validateAndNormalizeEventRecord(record);

    try {
      // Read lock is sufficient for send
      _sendFlushLock.readLock().lock();

      // Send the event to transport
      _transportProvider.send(task.getDatastreamDestination().getConnectionString(), record);

      // Update the checkpoint for the task/partition
      _latestCheckpoints.get(task).put(record.getPartition(), record.getCheckpoint());

      // Dirty the flag
      _pendingCheckpoint = true;
    } catch (TransportException e) {
      LOG.info(String.format("Failed send the event %s exception %s", record, e));
      throw new RuntimeException("Failed to send: " + record, e);
    } finally {
      _sendFlushLock.readLock().unlock();
    }
  }

  public void flush() {
    if (!_pendingCheckpoint) {
      return;
    }

    try {
      // Write lock is needed for flush
      _sendFlushLock.writeLock().lock();

      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("Staring transport flush, tasks = [%s].", _tasks));
      }

      _transportProvider.flush();

      if (LOG.isDebugEnabled()) {
        LOG.debug("Transport has been successfully flushed.");
      }

      if (_checkpointPolicy == CheckpointPolicy.DATASTREAM) {
        try {
          LOG.info(String.format("Start committing checkpoints = %s, tasks = [%s].", _latestCheckpoints, _tasks));

          // Populate checkpoint map for checkpoint provider
          Map<DatastreamTask, String> committed = new HashMap<>();
          _latestCheckpoints.forEach((task, cpMap) -> committed.put(task, JsonUtils.toJson(cpMap)));
          _checkpointProvider.commit(committed);

          // Update the safe checkpoints for the task
          _latestCheckpoints.forEach((task, cpMap) -> _safeCheckpoints.put(task, new HashMap<>(cpMap)));

          LOG.info("Checkpoints have been successfully committed: " + _safeCheckpoints);
        } catch (Throwable t) {
          throw new RuntimeException(String.format("Checkpoint commit failed, tasks = [%s].", _tasks), t);
        }
      }

      // Clear the dirty flag
      _pendingCheckpoint = false;
    } catch (Throwable t) {
      throw new RuntimeException(String.format("Failed to flush transport, tasks = [%s].", _tasks), t);
    } finally {
      _sendFlushLock.writeLock().unlock();
    }
  }

  /**
   * @return a map of safe checkpoints per task, per partition.
   * This is internally used by DatastreamTaskImpl. Connectors
   * are expected to all {@link DatastreamTask#getCheckpoints()}.
   */
  public Map<DatastreamTask, Map<Integer, String>> getSafeCheckpoints() {
    try {
      // Read lock is needed as safeCheckpoint can be modified
      // by flush and updateTasks both of which acquire writeLock
      _sendFlushLock.readLock().lock();

      // Give back read-only checkpoints
      return Collections.unmodifiableMap(_safeCheckpoints);
    } finally {
      _sendFlushLock.readLock().unlock();
    }
  }

  /**
   * shutdown should only be called when all tasks associated with it are out of mission.
   * It is the responsibility of the {@link EventProducerPool} to ensure this.
   */
  public void shutdown() {
    if (_shutdownCompleted) {
      return;
    }

    LOG.info("Shutting down event producer for " + _tasks);

    _shutdownRequested = true;
    _checkpointHandler.shutdown();

    try {
      _transportProvider.close();
    } catch (TransportException e) {
      LOG.warn("Closing the TransportProvider failed with exception", e);
    }

    _shutdownCompleted = true;
  }

  @Override
  public String toString() {
    return "EventProducer tasks=" + _tasks;
  }
}
