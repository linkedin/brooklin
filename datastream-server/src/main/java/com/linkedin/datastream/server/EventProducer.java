package com.linkedin.datastream.server;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.ErrorLogger;
import com.linkedin.datastream.common.JsonUtils;
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
  /**
   * Policy for checkpoint handling
   */
  enum CheckpointPolicy {
    DATASTREAM,
    CUSTOM
  }

  private static final String MODULE = EventProducer.class.getName();
  public static final String INVALID_CHECKPOINT = "";
  public static final String CHECKPOINT_PERIOD_MS = "checkpointPeriodMs";
  public static final Integer DEFAULT_CHECKPOINT_PERIOD_MS = 1000 * 60; // 1 minute
  public static final Integer DEFAULT_SHUTDOWN_POLL_MS = 1000 * 60; // 1 minute

  private static final AtomicInteger PRODUCER_ID_SEED = new AtomicInteger(0);

  private final int _producerId;
  private final Consumer<EventProducer> _onUnrecoverableError;
  private final Logger _logger;

  private final TransportProvider _transportProvider;
  private final CheckpointProvider _checkpointProvider;
  private final CheckpointPolicy _checkpointPolicy;

  // This stores the checkpoints that have been recently acknowledged by
  // the transport (via onSendCallback) and are safe to be committed to
  // the checkpoint provider. We instantiate it as a concurrentHashMap
  // because we expose safeCheckpoints by reference such that connectors
  // can be accessing the checkpoints while event procuder mutates them,
  // which is safe but can cause ConcurrentModificationException since
  // there is no synchronization between them. This is only be a problem
  // for CUSTOM checkpoint policy as DATASTREAM policy users only access
  // the checkpoints in the startup phase.
  private final ConcurrentHashMap<DatastreamTask, Map<Integer, String>> _safeCheckpoints;

  // Helper for periodical flush/checkpoint operation
  private final CheckpointHandler _checkpointHandler;

  private volatile boolean _shutdownCompleted = false;
  private volatile boolean _shutdownRequested = false;

  // Flag indicating if there are acknowledged checkpoints pending to
  // be committed to the checkpoint provider.
  private final AtomicBoolean _pendingCheckpoints = new AtomicBoolean(false);

  // This lock synchronizes various operations of the producer:
  //  readers: getSafeCheckpoints, onSendCallback
  //  writer: assign/unassignTasks, flushAndCheckpoint
  // The lock pretects _safeCheckpoints and _pendingCheckpoints
  private ReentrantReadWriteLock _checkpointRWLock;

  private static final Counter TOTAL_EVENTS_PRODUCED = new Counter();
  private static final Counter EVENTS_PRODUCED_WITHIN_SLA = new Counter();
  private static final String AVAILABILITY_THRESHOLD_SLA_MS = "availabilityThresholdSlaMs";
  private static final String DEFAULT_AVAILABILITY_THRESHOLD_SLA_MS = "60000"; // 1 minute
  private final int _availabilityThresholdSlaMs;

  /**
   * Manages the periodic checkpointing operation.
   */
  class CheckpointHandler implements Runnable {
    private final long _periodMs;
    private final ScheduledExecutorService _executor;

    public CheckpointHandler(Properties config) {
      VerifiableProperties props = new VerifiableProperties(config);
      _periodMs = props.getLong(CHECKPOINT_PERIOD_MS, DEFAULT_CHECKPOINT_PERIOD_MS);
      _executor = new ScheduledThreadPoolExecutor(1);
      _executor.scheduleAtFixedRate(this, 0, _periodMs, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
      _logger.info("Shutting down checkpoint handler, tasks = " + getTaskString());

      try {
        _executor.shutdown();
        _executor.awaitTermination(DEFAULT_SHUTDOWN_POLL_MS, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        _logger.warn("Checkpoint handler shutdown is interrupted, forcing termination.");
        _executor.shutdownNow();
      }

      // Final checkpointing
      doCheckpoint();

      _logger.info("Checkpoint handler is shutdown.");
    }

    private void doCheckpoint() {
      try {
        _logger.info(String.format("Checkpoint handler started, tasks = [%s].", getTaskString()));
        EventProducer.this.flushAndCheckpoint();
        _logger.info("Checkpoint handler finished.");
      } catch (Exception e) {
        // We need to handle catch all exceptions otherwise executor suppresses future runs.
        _logger.error(
            String.format("Checkpoint handler failed, will retry in %d ms, reason = %s", _periodMs, e.getMessage()), e);
      }
    }

    @Override
    public void run() {
      doCheckpoint();
    }
  }

  /**
   * Construct an EventProducer instance.
   * @param transportProvider event transport
   * @param checkpointProvider checkpoint store
   * @param config global config
   * @param customCheckpointing decides whether Producer should use custom checkpointing or the datastream server
   *                            provided checkpointing.
   * @param onUnrecoverableError callback to be triggered when there is unrecoverable errors from the transport
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
    _safeCheckpoints = new ConcurrentHashMap<>();
    _onUnrecoverableError = onUnrecoverableError;

    // Start checkpoint handler
    _checkpointHandler = new CheckpointHandler(config);

    _producerId = PRODUCER_ID_SEED.getAndIncrement();
    _logger = LoggerFactory.getLogger(String.format("%s:%s", MODULE, _producerId));

    _availabilityThresholdSlaMs =
        Integer.parseInt(config.getProperty(AVAILABILITY_THRESHOLD_SLA_MS, DEFAULT_AVAILABILITY_THRESHOLD_SLA_MS));

    _logger.info(String
        .format("Created event producer with customCheckpointing=%s, sendCallback=%s", customCheckpointing,
            onUnrecoverableError != null));
  }

  public int getProducerId() {
    return _producerId;
  }

  private String getTaskString() {
    return _safeCheckpoints.keySet().toString();
  }

  private Map<Integer, String> loadCheckpoints(DatastreamTask task) {
    Map<DatastreamTask, String> committed = _checkpointProvider.getCommitted(Collections.singletonList(task));

    // Instruct jackson to convert string keys to integer
    TypeReference<ConcurrentHashMap<Integer, String>> typeRef =
        new TypeReference<ConcurrentHashMap<Integer, String>>() {
        };

    String cpString = committed.get(task);
    ConcurrentHashMap<Integer, String> cpMap;
    if (!StringUtils.isBlank(cpString)) {
      // Deserialize checkpoints from persisted JSON
      try {
        cpMap = JsonUtils.fromJson(cpString, typeRef);
      } catch (Exception e) {
        throw new DatastreamRuntimeException("Failed to load checkpoints from: " + cpString + ", task=" + task, e);
      }
    } else {
      // Brand new task without any prior checkpoints
      cpMap = new ConcurrentHashMap<>();
      task.getPartitions().forEach(partition -> cpMap.put(partition, INVALID_CHECKPOINT));
    }

    return cpMap;
  }

  private void validateEventRecord(DatastreamProducerRecord record) {
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
    Validate.notNull(task, "null task");

    if (!_safeCheckpoints.containsKey(task)) {
      _logger.warn("Task is not assigned: " + task);
      return;
    }

    // Flush can be done outside critical section
    flushAndCheckpoint();

    try {
      _checkpointRWLock.writeLock().lock();
      _safeCheckpoints.remove(task);
    } finally {
      _checkpointRWLock.writeLock().unlock();
    }
  }

  /**
   * For a new task assignment, this method should be called to update the internal
   * checkpoint structures to accommodate for newly assigned and unassigned tasks.
   */
  public void assignTask(DatastreamTask task) {
    Validate.notNull(task, "null task");

    if (_safeCheckpoints.containsKey(task)) {
      _logger.warn("Task is already assigned: " + task);
      return;
    }

    // checkpoint loading can be done ouside critical section
    Map<Integer, String> checkpoints = loadCheckpoints(task);

    try {
      _checkpointRWLock.writeLock().lock();
      _safeCheckpoints.put(task, checkpoints);
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

    validateEventRecord(record);

    try {
      // No locking is needed as send does not touch checkpoint at all
      _transportProvider.send(task.getDatastreamDestination().getConnectionString(), record,
          (metadata, exception) -> onSendCallback(metadata, exception, sendCallback, task, record));
    } catch (Exception e) {
      String errorMessage = String.format("Failed send the event %s exception %s", record, e);
      _logger.warn(errorMessage, e);
      throw new DatastreamRuntimeException(errorMessage, e);
    }
  }

  private void onSendCallback(DatastreamRecordMetadata metadata, Exception exception, SendCallback sendCallback,
      DatastreamTask task, DatastreamProducerRecord record) {

    SendFailedException sendFailedException = null;

    // Notify the eventProducerPool first which will recreate a new event producer
    if (exception != null) {
      sendFailedException = new SendFailedException(_safeCheckpoints);

      if (_onUnrecoverableError != null) {
        _onUnrecoverableError.accept(this);
      }
    } else {
      // Report availability metrics
      if (System.currentTimeMillis() - record.getEventsTimestamp() <= _availabilityThresholdSlaMs) {
        EVENTS_PRODUCED_WITHIN_SLA.inc();
      }
      TOTAL_EVENTS_PRODUCED.inc();

      try {
        // read-lock is sufficient as messages in the same partition is expected to be acknowledged
        // in order by the transport such that we can allow concurrent acks of other tasks/partitions.
        _checkpointRWLock.readLock().lock();
        if (!_safeCheckpoints.containsKey(task)) {
          _logger.warn(String.format("Event of unknown task is being acknowledged: task=%s, record=%s", task, record));
        } else {
          _safeCheckpoints.get(task).put(metadata.getPartition(), record.getCheckpoint());
          _pendingCheckpoints.set(true);
        }
      } finally {
        _checkpointRWLock.readLock().unlock();
      }
    }

    // Inform the connector about the success or failure, In the case of failure,
    // the connector is expected retry and go back to the last checkpoint.
    if (sendCallback != null) {
      sendCallback.onCompletion(metadata, sendFailedException);
    }

    // Shutdown the producer right away
    if (exception != null) {
      shutdown();
    }
  }

  /**
   * Flush the pending unset events in the transport and commit the acknowledged
   * checkpoints (via onSendCallback) with checkpoint provider afterwards.
   *
   * The method is maded synchronized to prevent concurrent execution by:
   *  - unassignTask
   *  - CheckpointHandler
   *  - external flush request
   */
  public synchronized void flushAndCheckpoint() {
    String tasks = getTaskString();

    // Step 1: flush the transport to gather ACKs
    try {
      // Flush can and must be done without holding writer-lock which is needed by onSendCallback
      _logger.info(String.format("Staring transport flush, tasks = [%s].", tasks));
      _transportProvider.flush();
      _logger.info("Transport has been successfully flushed.");
    } catch (Exception e) {
      String msg = String.format("Failed to flush transport, tasks = [%s].", tasks);
      ErrorLogger.logAndThrowDatastreamRuntimeException(_logger, msg, e);
    }

    if (_checkpointPolicy != CheckpointPolicy.DATASTREAM) {
      return;
    }

    if (!_pendingCheckpoints.get()) {
      _logger.info("No changes in checkpoints, skipping commit.");
      return;
    }

    // Step 2: serialize checkpoints to be committed
    Map<DatastreamTask, String> committed;
    try {
      // Hold write-lock to prevent:
      //  1) _safeCheckpoints mutations (unassignTasks); and
      //  2) _pendingCheckpoints mutations (onSendCallback)
      _checkpointRWLock.writeLock().lock();
      committed = new HashMap<>();
      _safeCheckpoints.forEach((task, cpMap) -> committed.put(task, JsonUtils.toJson(cpMap)));
      _pendingCheckpoints.set(false);
    } finally {
      _checkpointRWLock.writeLock().unlock();
    }

    // Step 3: commit the serialized checkpoints to checkpoint provider
    try {
      // Commit can be safely performed outside of critical section
      _logger.info(String.format("Start committing checkpoints = %s, tasks = [%s].", committed, tasks));
      _checkpointProvider.commit(committed);
      _logger.info("Checkpoints have been successfully committed");
    } catch (Exception e) {
      String errorMessage = String.format("Checkpoint commit failed, tasks = [%s].", tasks);
      ErrorLogger.logAndThrowDatastreamRuntimeException(_logger, errorMessage, e);
    }
  }

  /**
   * @return a map of safe checkpoints per task, per partition.
   * This is internally used by DatastreamTaskImpl. Connectors
   * are expected to all {@link DatastreamTask#getCheckpoints()}.
   */
  public Map<DatastreamTask, Map<Integer, String>> getSafeCheckpoints() {
    try {
      // read-lock is needed to synchronize with (un)assignTask
      _checkpointRWLock.readLock().lock();

      // Give back read-only checkpoints. DatastreamTaskImpl will
      // make the per-task maps unmodifiable before handing them
      // of connectors.
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

    _logger.info("Shutting down event producer for " + getTaskString());

    _shutdownRequested = true;
    _checkpointHandler.shutdown();

    try {
      _transportProvider.close();
    } catch (TransportException e) {
      _logger.warn("Closing the TransportProvider failed with exception", e);
    }

    _shutdownCompleted = true;
  }

  @Override
  public String toString() {
    return "EventProducer tasks=" + getTaskString();
  }

  public static Map<String, Metric> getMetrics() {
    Map<String, Metric> metrics = new HashMap<>();

    metrics.put(MetricRegistry.name(EventProducer.class.getSimpleName(), "eventsProducedWithinSla"),
        EVENTS_PRODUCED_WITHIN_SLA);
    metrics.put(MetricRegistry.name(EventProducer.class.getSimpleName(), "totalEventsProduced"), TOTAL_EVENTS_PRODUCED);

    return Collections.unmodifiableMap(metrics);
  }
}
