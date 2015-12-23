package com.linkedin.datastream.server;

import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.api.schemaregistry.SchemaRegistryException;
import com.linkedin.datastream.server.api.schemaregistry.SchemaRegistryProvider;
import com.linkedin.datastream.server.api.transport.TransportException;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.providers.CheckpointProvider;

import org.apache.avro.Schema;
import org.apache.commons.lang.Validate;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * DatastreamEventProducerImpl is the default implementation of {@link DatastreamEventProducer}.
 * It allows connectors to send events to the transport and handles save/restore checkpoints
 * automatically if {@link DatastreamEventProducerImpl.CheckpointPolicy#DATASTREAM} is specified.
 * Otherwise, it exposes the safe checkpoints which are guaranteed to have been flushed.
 */
public class DatastreamEventProducerImpl implements DatastreamEventProducer {
  private static final Logger LOG = LoggerFactory.getLogger(DatastreamEventProducerImpl.class);

  /**
   * Policy for checkpoint handling
   */
  enum CheckpointPolicy { DATASTREAM, CUSTOM }

  public static final String INVALID_CHECKPOINT = "INVALID_CHECKPOINT";
  public static final String CHECKPOINT_PERIOD_MS = "checkpointPeriodMs";
  public static final Integer DEFAULT_CHECKPOINT_PERIOD_MS = 1000;
  public static final Integer SHUTDOWN_POLL_MS = 1000;
  public static final Integer SHUTDOWN_POLL_PERIOD_MS = 50;

  // List of tasks the producer is responsible for
  private final List<DatastreamTask> _tasks;

  private final TransportProvider _transportProvider;
  private final SchemaRegistryProvider _schemaRegistryProvider;
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

  private Map<DatastreamTask, String> _pendingCheckpoints;

  // Helper for periodical flush/checkpoint operations
  private final CheckpointHandler _checkpointHandler;

  private volatile boolean _pendingCheckpoint = false;
  private volatile boolean _shutdownRequested = false;

  /**
   * Flush transport periodically and save checkpoints.
   * Default period is 1 second.
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

      // Initial shutdown and interrupt the thread
      _executor.shutdown();

      // Poll for 1 second for the thread to actually exit
      PollUtils.poll(() -> _executor.isTerminated(), SHUTDOWN_POLL_MS, SHUTDOWN_POLL_PERIOD_MS);

      // Final flush
      flushAndCheckpoint();

      LOG.info(String.format("Shutdown finished, tasks = [%s].", _tasks));
    }

    private void flushAndCheckpoint() {
      try {
        flush();
        LOG.debug(String.format("Checkpoint handler exited, tasks = [%s].", _tasks));
      } catch (Throwable t){
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
   * Construct a DatastreamEventProducerImpl instance.
   * @param tasks list of tasks which have the same destination
   * @param transportProvider event transport
   * @param schemaRegistryProvider
   * @param checkpointProvider checkpoint store
   * @param config global config
   * @param customCheckpointing decides whether Producer should use custom checkpointing or the datastream server
   *                            provided checkpointing.
   */
  public DatastreamEventProducerImpl(List<DatastreamTask> tasks,
                                     TransportProvider transportProvider,
                                     SchemaRegistryProvider schemaRegistryProvider,
                                     CheckpointProvider checkpointProvider,
                                     Properties config,
                                     boolean customCheckpointing) {
    Validate.notNull(tasks, "null tasks");
    Validate.notNull(transportProvider, "null transport provider");
    Validate.notNull(checkpointProvider, "null checkpoint provider");
    Validate.notNull(config, "null config");
    Validate.notEmpty(tasks, "empty task list");

    DatastreamTask task0 = tasks.get(0);
    Set<Integer> knownPartitions = new HashSet<>();
    for (DatastreamTask task : tasks) {
      // Ensure all tasks are for the same destination
      task.getDatastreamDestination().equals(task0.getDatastreamDestination());

      // DatastreamTask should include at least one partition.
      // This should be ensured by DatastreamTaskImpl's ctor.
      Validate.notEmpty(task.getPartitions(), "null or empty partitions in: " + task);

      // Ensure no duplicate partitions exist
      for (Integer partition : task.getPartitions()) {
        Validate.isTrue(!knownPartitions.contains(partition), "duplicate partition: " + partition);
        knownPartitions.add(partition);
      }
    }

    _tasks = tasks;
    _transportProvider = transportProvider;
    _schemaRegistryProvider = schemaRegistryProvider;
    _checkpointProvider = checkpointProvider;

    _checkpointPolicy = customCheckpointing ? CheckpointPolicy.CUSTOM : CheckpointPolicy.DATASTREAM;

    _checkpointHandler = new CheckpointHandler(config);

    // For DATASTREAM checkpoint policy, load initial checkpoints
    if (_checkpointPolicy == CheckpointPolicy.DATASTREAM) {
      loadCheckpoints();
    }

    _pendingCheckpoints = new HashMap<>();
    _tasks.forEach(task -> _pendingCheckpoints.put(task, INVALID_CHECKPOINT));

    // This can happen for first time run or custom checkpointing
    if (_safeCheckpoints == null || _safeCheckpoints.size() == 0) {
      _safeCheckpoints = new HashMap<>();
      for (DatastreamTask task : tasks) {
        Map<Integer, String> checkpoints = new HashMap<>();
        _safeCheckpoints.put(task, checkpoints);
        task.getPartitions().forEach(partition -> checkpoints.put(partition, INVALID_CHECKPOINT));
      }
    }

    _latestCheckpoints = new HashMap<>(_safeCheckpoints);
  }

  private void loadCheckpoints() {
    _safeCheckpoints = new HashMap<>();

    Map<DatastreamTask, String> checkpoints = _checkpointProvider.getCommitted(_tasks);

    // Instruct jackson to convert string keys to integer
    TypeReference typeRef = new TypeReference<HashMap<Integer, String>>() {};

    for (DatastreamTask task : checkpoints.keySet()) {
      String cpString = checkpoints.get(task);
      try {
        _safeCheckpoints.put(task, JsonUtils.fromJson(cpString, typeRef));
      } catch (Exception e) {
        throw new IllegalArgumentException(String.format(
                "Failed to load checkpoints, task = %s, checkpoint = %s, error = %s",
                task, cpString, e.getMessage()), e);
      }
    }
  }

  private void validateEventRecord(DatastreamEventRecord record) {
    Validate.notNull(record, "null event record.");
    Validate.notNull(record.getEvents(), "null event payload.");
    DatastreamTask task =  record.getDatastreamTask();
    Validate.notNull(task, "null event task.");
    Validate.notNull(record.getCheckpoint(), "null event checkpoint.");
    Validate.notEmpty(record.getDestination(), "invalid event destination.");
    Map<Integer, String> checkpoints = _latestCheckpoints.get(task);
    Validate.notNull(checkpoints, "unknown task: " + task);
  }

  /**
   * Send the event onto the underlying transport.
   *
   * @param record DatastreamEvent envelope
   */
  @Override
  public void send(DatastreamEventRecord record) {
    // Prevent sending if we have been shutdown
    if (_shutdownRequested) {
      throw new IllegalStateException("send() is not allowed on a shutdown producer");
    }

    validateEventRecord(record);

    try {
      // Send the event to transport
      _transportProvider.send(record);

      // Update the checkpoint for the task/partition
      _latestCheckpoints.get(record.getDatastreamTask()).put(record.getPartition(), record.getCheckpoint());

      // Dirty the flag
      _pendingCheckpoint = true;
    } catch (TransportException e) {
      LOG.info(String.format("Failed send the event %s exception %s", record, e));
      throw new RuntimeException("Failed to send: " + record, e);
    }
  }

  /**
   * Register the schema in schema registry. If the schema already exists in the registry
   * Just return the schema Id of the existing
   * @param schema Schema that needs to be registered.
   * @return
   *   SchemaId of the registered schema.
   */
  @Override
  public String registerSchema(Schema schema)
      throws SchemaRegistryException {
    if(_schemaRegistryProvider != null) {
      return _schemaRegistryProvider.registerSchema(schema);
    } else {
      LOG.info("SchemaRegistryProvider is not configured, so registerSchema is not supported");
      throw new RuntimeException("SchemaRegistryProvider is not configured, So registerSchema is not supported");
    }
  }

  @Override
  public synchronized void flush() {
    if (!_pendingCheckpoint) {
      return;
    }

    try {
      LOG.info(String.format("Staring transport flush, tasks = [%s].", _tasks));

      _transportProvider.flush();

      LOG.info("Transport has been successfully flushed.");
    } catch (Throwable t) {
      throw new RuntimeException(String.format("Flush failed, tasks = [%s].", _tasks), t);
    }
    if (_checkpointPolicy == CheckpointPolicy.DATASTREAM) {
      try {
        LOG.info(String.format("Start committing checkpoints, cpMap = %s, tasks = [%s].",
                _pendingCheckpoints, _tasks));

        // Populate checkpoint map for checkpoint provider
        _latestCheckpoints.keySet().forEach(task -> _pendingCheckpoints.put(
                task, JsonUtils.toJson(_latestCheckpoints.get(task))));

        _checkpointProvider.commit(_pendingCheckpoints);

        // Update the safe checkpoints for the task
        _latestCheckpoints.keySet().forEach(task -> _safeCheckpoints.put(task, _latestCheckpoints.get(task)));

        LOG.info("Checkpoints have been successfully committed.");
      } catch (Throwable t) {
        throw new RuntimeException(String.format("Checkpoint commit failed, tasks = [%s].", _tasks), t);
      }
    }

    _pendingCheckpoint = false;

    LOG.info("Safe checkpoints: " + _safeCheckpoints);
  }

  /**
   * @return a map of safe checkpoints per task, per partition.
   * This is internally used by DatastreamTaskImpl. Connectors
   * are expected to all {@link DatastreamTask#getCheckpoints()}.
   */
  public synchronized Map<DatastreamTask, Map<Integer, String>> getSafeCheckpoints() {
    return _safeCheckpoints;
  }

  /**
   * shutdown should only be called when all tasks associated with it are out of mission.
   * It is the responsibility of the {@link EventProducerPool} to ensure this.
   */
  public void shutdown() {
    try {
      _transportProvider.close();
    } catch (TransportException e) {
      LOG.warn("Closing the TransportProvider failed with exception", e);
    }
    _checkpointHandler.shutdown();
  }
}
