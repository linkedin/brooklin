package com.linkedin.datastream.server;

import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.providers.CheckpointProvider;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * DatastreamEventProducerImpl is the default implementation of {@link DatastreamEventProducer}.
 * It allows connectors to send events to the transport and handles save/restore checkpoints
 * automatically if {@link DatastreamEventProducer.CheckpointPolicy#DATASTREAM} is specified.
 * Otherwise, it exposes the safe checkpoints which are guaranteed to have been flushed.
 */
public class DatastreamEventProducerImpl implements DatastreamEventProducer {
  private static final Logger LOG = LoggerFactory.getLogger(DatastreamEventProducerImpl.class);

  public static final String CHECKPOINT_PERIOD_MS = "checkpointPeriodMs";
  public static final Integer DEFAULT_CHECKPOINT_PERIOD_MS = 1000;
  public static final Integer SHUTDOWN_POLL_MS = 1000;
  public static final Integer SHUTDOWN_POLL_PERIOD_MS = 50;

  // List of tasks the producer is responsible for
  private final List<DatastreamTask> _tasks;

  private final TransportProvider _transportProvider;
  private final CheckpointProvider _checkpointProvider;
  private final CheckpointPolicy _checkpointPolicy;

  // This stores the latest "dirty" checkpoints that we received
  // from send() call but haven't been flushed on the transport.
  private final Map<DatastreamTask, String> _latestCheckpoints;

  // This stores the checkpoints that have been recently flushed such that
  // are safe to be committed to the source consumption tracking system.
  private Map<DatastreamTask, String> _safeCheckpoints;

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
    private final String _taskDesc;
    private final ScheduledExecutorService _executor;

    public CheckpointHandler(Properties config) {
      VerifiableProperties props = new VerifiableProperties(config);
      _periodMs = props.getLong(CHECKPOINT_PERIOD_MS, DEFAULT_CHECKPOINT_PERIOD_MS);

      // Create a string representation of all the tasks for logging
      List<String> tasks = new ArrayList<>(_tasks.size());
      _tasks.forEach(task -> tasks.add(task.toString()));
      _taskDesc = "[" + String.join(",", tasks) + "]";

      _executor = new ScheduledThreadPoolExecutor(1);
      _executor.scheduleAtFixedRate(this, 0, _periodMs, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
      LOG.info(String.format("Shutdown requested, tasks = [%s].", _taskDesc));

      _shutdownRequested = true;

      // Initial shutdown and interrupt the thread
      // run() takes care of final flush.
      _executor.shutdownNow();

      // Poll for 1 second for the thread to actually exit
      PollUtils.poll(() -> _executor.isTerminated(), SHUTDOWN_POLL_MS, SHUTDOWN_POLL_PERIOD_MS);

      LOG.info(String.format("Shutdown finished, tasks = [%s].", _taskDesc));
    }

    /**
     * Trigger a flush on the underlying transport
     * and save the checkpoints after completion
     */
    private void flushAndMakeCheckpoints() throws DatastreamException {
      synchronized (DatastreamEventProducerImpl.this)
      {
        if (!_pendingCheckpoint) {
          return;
        }

        try {
          LOG.info(String.format("Flush started, tasks = [%s].", _taskDesc));

          _transportProvider.flush();

          LOG.info("Flush ended.");
        } catch (Throwable t) {
          throw new DatastreamException(String.format("Flush failed, tasks = [%s].", _taskDesc), t);
        }

        if (_checkpointPolicy == CheckpointPolicy.DATASTREAM) {
          try {
            LOG.info(String.format("Checkpoint commit started, tasks = [%s].", _taskDesc));

            _checkpointProvider.commit(_latestCheckpoints);

            LOG.info("Commit ended.");
          } catch (Throwable t) {
            throw new DatastreamException(String.format("Checkpoint commit failed, tasks = [%s].", _taskDesc), t);
          }
        }

        // Current checkpoints become safe after flush
        _safeCheckpoints.putAll(_latestCheckpoints);

        _pendingCheckpoint = false;

        LOG.debug("Safe checkpoints: " + _latestCheckpoints);
      }
    }

    @Override
    public void run() {
      if (_shutdownRequested) {
        return;
      }

      try {
        flushAndMakeCheckpoints();
        LOG.info(String.format("Checkpoint handler exited, tasks = [%s].", _taskDesc));
      } catch (Throwable t){
        // Since have have handled all exceptions here, there would be no exceptions leaked
        // to the executor, which will reschedules as usual so no explicit retry is needed.
        LOG.error(String.format("Checkpoint failed, will retry after %d ms, reason = %s", _periodMs, t.getMessage()), t);
      }
    }
  }

  /**
   * Construct a DatastreamEventProducerImpl instance.
   * @param tasks list of tasks which have the same destination
   * @param transportProvider event transport
   * @param checkpointProvider checkpoint store
   * @param config global config
   */
  public DatastreamEventProducerImpl(List<DatastreamTask> tasks,
                                     TransportProvider transportProvider,
                                     CheckpointProvider checkpointProvider,
                                     Properties config) {
    Validate.notNull(tasks, "null tasks");
    Validate.notNull(transportProvider, "null transport provider");
    Validate.notNull(checkpointProvider, "null checkpoint provider");
    Validate.notNull(config, "null config");
    Validate.notEmpty(tasks, "empty task list");

    // Verify all tasks have the same source
    DatastreamTask task0 = tasks.get(0);
    for (DatastreamTask task: tasks) {
      task.getDatastreamDestination().equals(task0.getDatastreamSource());
    }

    _tasks = tasks;
    _transportProvider = transportProvider;
    _checkpointProvider = checkpointProvider;

    // TODO: always do checkpoint for now
    _checkpointPolicy = CheckpointPolicy.DATASTREAM;

    _checkpointHandler = new CheckpointHandler(config);

    // For DATASTREAM checkpoint policy, load initial checkpoints
    if (_checkpointPolicy == CheckpointPolicy.DATASTREAM) {
      _safeCheckpoints = _checkpointProvider.getCommitted(_tasks);
    }

    // If there is no initial checkpoints or CUSTOM policy, create empty checkpoints.
    if (_safeCheckpoints == null) {
      _safeCheckpoints = new HashMap<>();
    }

    // Starts off empty (no new checkpoints)
    _latestCheckpoints = new HashMap<>();
  }

  private void validateEventRecord(DatastreamEventRecord record) {
    Validate.notNull(record, "null event record.");
    Validate.notNull(record.getEvent(), "null event payload.");
    Validate.notNull(record.getDatastreamTask(), "null event task.");
    Validate.notNull(record.getCheckpoint(), "null event checkpoint.");
    Validate.notEmpty(record.getDestination(), "invalid event destination.");
  }

  /**
   * Send the event onto the underlying transport.
   *
   * @param record DatastreamEvent envelope
   */
  @Override
  public synchronized void send(DatastreamEventRecord record) {
    // Prevent sending if we have been shutdown
    if (_shutdownRequested) {
      return;
    }

    validateEventRecord(record);

    // Send the event to transport
    _transportProvider.send(record);

    // Update the checkpoint for the destination/partition
    _latestCheckpoints.put(record.getDatastreamTask(), record.getCheckpoint());

    // Dirty the flag
    _pendingCheckpoint = true;
  }

  @Override
  public synchronized Map<DatastreamTask, String> getSafeCheckpoints() {
    return _safeCheckpoints;
  }

  @Override
  public void shutdown() {
    _shutdownRequested = true;
    _checkpointHandler.shutdown();
  }
}
