package com.linkedin.datastream.server;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.ErrorLogger;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.api.transport.DatastreamRecordMetadata;
import com.linkedin.datastream.server.api.transport.SendCallback;
import com.linkedin.datastream.server.api.transport.SendFailedException;
import com.linkedin.datastream.server.api.transport.TransportException;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.providers.CheckpointProvider;


/**
 * EventProducer class uses the transport to send events and handles save/restore checkpoints
 * automatically if {@link EventProducer.CheckpointPolicy#DATASTREAM} is specified.
 * Otherwise, it exposes the safe checkpoints which are guaranteed to have been flushed.
 */
public class EventProducer {
  private static final Logger LOG = LoggerFactory.getLogger(EventProducer.class);
  private final Consumer<EventProducer> _onUnrecoverableError;

  private final String _producerid;

  /**
   * Policy for checkpoint handling
   */
  enum CheckpointPolicy {
    DATASTREAM,
    CUSTOM
  }

  public static final String INVALID_CHECKPOINT = "";
  public static final String CHECKPOINT_PERIOD_MS = "checkpointPeriodMs";
  public static final Integer DEFAULT_CHECKPOINT_PERIOD_MS = 1000 * 60; // 1 minute
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

  private volatile boolean _shutdownCompleted = false;
  private volatile boolean _shutdownRequested = false;

  // This read-write lock synchronizes send/flush/updateTasks
  //  a. send is reader because multiple send are safe to run concurrently
  //  b. flush/updateTasks is writer because they need to run in critical
  //     section as multiple structures are modified
  private ReentrantReadWriteLock _checkpointRWLock;

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
      } catch (Exception e) {
        // Since have have handled all exceptions here, there would be no exceptions leaked
        // to the executor, which will reschedules as usual so no explicit retry is needed.
        LOG.error(String.format("Checkpoint failed, will retry after %d ms, reason = %s", _periodMs, e.getMessage()), e);
      }
    }

    @Override
    public void run() {
      flushAndCheckpoint();
    }
  }

  /**
   * Construct an EventProducer instance.
   * @param transportProvider event transport
   * @param checkpointProvider checkpoint store
   * @param config global config
   * @param customCheckpointing decides whether Producer should use custom checkpointing or the datastream server
   *                            provided checkpointing.
   */
  public EventProducer(TransportProvider transportProvider, CheckpointProvider checkpointProvider, Properties config,
      boolean customCheckpointing, Consumer<EventProducer> onUnrecoverableError) {
    Validate.notNull(transportProvider, "null transport provider");
    Validate.notNull(checkpointProvider, "null checkpoint provider");
    Validate.notNull(config, "null config");

    // Using a fair lock to prevent flush getting starved by send
    _checkpointRWLock = new ReentrantReadWriteLock(/* fairness */ true);

    _transportProvider = transportProvider;
    _checkpointProvider = checkpointProvider;
    _checkpointPolicy = customCheckpointing ? CheckpointPolicy.CUSTOM : CheckpointPolicy.DATASTREAM;
    _tasks = new HashSet<>();
    _safeCheckpoints = new HashMap<>();
    _onUnrecoverableError = onUnrecoverableError;
    _latestCheckpoints = new HashMap<>();

    // Start checkpoint handler
    _checkpointHandler = new CheckpointHandler(config);

    _producerid = UUID.randomUUID().toString();

    LOG.info(String.format("Created event producer with customCheckpointing %s", customCheckpointing));
  }

  public String getProducerid() {
    return _producerid;
  }

  /**
   * This method should be called with sendFlushLock.writeLock acquired.
   */
  private Map<Integer, String> loadCheckpoints(DatastreamTask task) {
    Map<DatastreamTask, String> committed = _checkpointProvider.getCommitted(Collections.singletonList(task));
    final Map<Integer, String> cpMap;
    // Instruct jackson to convert string keys to integer
    TypeReference<HashMap<Integer, String>> typeRef = new TypeReference<HashMap<Integer, String>>() {
    };

    String cpString = committed.get(task);
    if (!StringUtils.isBlank(cpString)) {
      // Deserialize checkpoints from JSON
      try {
        cpMap = JsonUtils.fromJson(cpString, typeRef);
      } catch (Exception e) {
        throw new DatastreamRuntimeException("Failed to load checkpoints from json: " + cpString + ", task=" + task, e);
      }
    } else {
      // Brand new task without checkpoints
      cpMap = new HashMap<>();
      task.getPartitions().forEach(partition -> cpMap.put(partition, INVALID_CHECKPOINT));
    }

    return cpMap;
  }

  private void validateAndNormalizeEventRecord(DatastreamProducerRecord record) {
    Validate.notNull(record, "null event record.");
    Validate.notNull(record.getEvents(), "null event payload.");
    Validate.notNull(record.getCheckpoint(), "null event checkpoint.");

    for (Pair<byte[], byte[]> event : record.getEvents()) {
      Validate.notNull(event, "null event");
      Validate.notNull(event.getKey(), "null key");
      Validate.notNull(event.getValue(), "null value");
    }
  }

  public CheckpointPolicy getCheckpointPolicy() {
    return _checkpointPolicy;
  }

  public void unassignTask(DatastreamTask task) {
    try {
      // Write lock is required
      _checkpointRWLock.writeLock().lock();

      // Flush any outstanding events
      flush();
      _safeCheckpoints.remove(task);
      _latestCheckpoints.remove(task);
      _tasks.remove(task);
    } finally {
      _checkpointRWLock.writeLock().unlock();
    }
  }

  /**
   * For a new task assignment, this method should be called to update the internal
   * checkpoint structures to accommodate for newly assigned and unassigned tasks.
   */
  public void assignTask(DatastreamTask task) {
    Validate.notNull(task, "null assigned task");

    try {
      // Write lock is required
      _checkpointRWLock.writeLock().lock();

      // Add newly assigned tasks and populate checkpoints
      _tasks.add(task);

      Map<Integer, String> checkpoints = loadCheckpoints(task);
      _safeCheckpoints.put(task, checkpoints);
      _latestCheckpoints.put(task, new HashMap<>(checkpoints)); // make a new copy
    } finally {
      _checkpointRWLock.writeLock().unlock();
    }
  }

  /**
   * Send the event onto the underlying transport.
   *
   * @param record DatastreamEvent envelope
   * @param sendCallback
   */
  public void send(DatastreamTask task, DatastreamProducerRecord record, SendCallback sendCallback) {
    // Prevent sending if we have been shutdown
    if (_shutdownRequested) {
      throw new IllegalStateException("send() is not allowed on a producer that is already shutdown.");
    }

    validateAndNormalizeEventRecord(record);

    try {
      // No locking is needed as send does not touch checkpoint
      _transportProvider.send(task.getDatastreamDestination().getConnectionString(), record,
          (metadata, exception) -> onSendCallback(metadata, exception, sendCallback, task, record));
    } catch (Exception e) {
      String errorMessage = String.format("Failed send the event %s exception %s", record, e);
      LOG.warn(errorMessage, e);
      throw new DatastreamRuntimeException(errorMessage, e);
    }
  }

  private void onSendCallback(DatastreamRecordMetadata metadata, Exception exception, SendCallback sendCallback,
      DatastreamTask task, DatastreamProducerRecord record) {

    SendFailedException sendFailedException = null;

    // Shutdown the producer right away
    if (exception != null) {
      shutdown();
      sendFailedException = new SendFailedException(_safeCheckpoints);
    }

    // Inform the connector about the success or failure, In the case of failure, the connector will go back to the
    // last checkpoint.
    if (sendCallback != null) {
      sendCallback.onCompletion(metadata, sendFailedException);
    }

    // If the send failed, then shutdown the producer, Notify the eventProducerPool and the connector sendCallback
    if (exception != null) {
      if (_onUnrecoverableError != null) {
        _onUnrecoverableError.accept(this);
      }
    } else {
      // Update the checkpoint for the task/partition
      _latestCheckpoints.get(task).put(metadata.getPartition(), record.getCheckpoint());
    }
  }

  public void flush() {
    try {
      // Write lock is needed for flush
      _checkpointRWLock.writeLock().lock();

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

          // Make a copy to ensure safeCheckpoints is consistent with the committed ones
          Map<DatastreamTask, Map<Integer, String>> checkpoints = new HashMap<>(_latestCheckpoints);

          if (checkpoints.equals(_safeCheckpoints)) {
            LOG.info("No changes in checkpoints, skipping commit.");
            return;
          }

          // Populate checkpoint map for checkpoint provider
          Map<DatastreamTask, String> committed = new HashMap<>();
          checkpoints.forEach((task, cpMap) -> committed.put(task, JsonUtils.toJson(cpMap)));
          _checkpointProvider.commit(committed);

          // Update the safe checkpoints for the task
          checkpoints.forEach((task, cpMap) -> _safeCheckpoints.put(task, new HashMap<>(cpMap)));

          LOG.info("Checkpoints have been successfully committed: " + _safeCheckpoints);
        } catch (Exception e) {
          String errorMessage = String.format("Checkpoint commit failed, tasks = [%s].", _tasks);
          ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, e);
        }
      }
    } catch (Exception e) {
      String errorMessage = String.format("Failed to flush transport, tasks = [%s].", _tasks);
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, e);
    } finally {
      _checkpointRWLock.writeLock().unlock();
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
      _checkpointRWLock.readLock().lock();

      // Give back read-only checkpoints
      return Collections.unmodifiableMap(_safeCheckpoints);
    } finally {
      _checkpointRWLock.readLock().unlock();
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
