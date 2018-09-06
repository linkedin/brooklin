package com.linkedin.datastream.connectors.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.common.ThreadUtils;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.providers.CheckpointProvider;


/**
 * Connector reads the text file line by line and produces events.
 * Connector uses the simple strategy, so the datastream can go to any instance. In a distributed environment,
 *   source should be  network files. local files can be used only on standalone environment.
 * Uses a single thread per file
 */
public class FileConnector implements Connector {
  private static final Logger LOG = LoggerFactory.getLogger(FileConnector.class);
  public static final String CONNECTOR_NAME = "file";
  public static final String CFG_MAX_EXEC_PROCS = "maxExecProcessors";
  public static final String CFG_NUM_PARTITIONS = "numPartitions";
  private static final String DEFAULT_MAX_EXEC_PROCS = "5";
  private static final Duration SHUTDOWN_TIMEOUT = Duration.ofMillis(5000);

  private final ExecutorService _executorService;
  private final int _numPartitions;
  private ConcurrentHashMap<DatastreamTask, FileProcessor> _fileProcessors;

  public FileConnector(Properties config) throws DatastreamException {
    _executorService =
        Executors.newFixedThreadPool(Integer.parseInt(config.getProperty(CFG_MAX_EXEC_PROCS, DEFAULT_MAX_EXEC_PROCS)));

    _numPartitions = Integer.parseInt(config.getProperty(CFG_NUM_PARTITIONS, "1"));
    _fileProcessors = new ConcurrentHashMap<>();
  }

  @Override
  public void start(CheckpointProvider checkpointProvider) {
    LOG.info("FileConnector started");
  }

  @Override
  public synchronized void stop() {
    // Stop all current processors
    stopProcessorForTasks(_fileProcessors.keySet());
    if (!ThreadUtils.shutdownExecutor(_executorService, SHUTDOWN_TIMEOUT, LOG)) {
      LOG.warn("Failed shut down cleanly.");
    }
    LOG.info("FileConnector is stopped.");
  }

  private void stopProcessorForTasks(Set<DatastreamTask> unassigned) {
    // Initiate stops for all unassigned tasks
    for (DatastreamTask task : unassigned) {
      FileProcessor processor = _fileProcessors.get(task);
      if (!processor.isStopped()) {
        processor.stop();
      }
    }

    // Ensure the processors have actually stopped
    for (DatastreamTask task : unassigned) {
      FileProcessor processor = _fileProcessors.get(task);
      if (!PollUtils.poll(processor::isStopped, 200, SHUTDOWN_TIMEOUT.toMillis())) {
        throw new RuntimeException("Failed to stop processor for " + task);
      }
      _fileProcessors.remove(task);
      LOG.info("Processor stopped for task: " + task);
    }
  }

  @Override
  public synchronized void onAssignmentChange(List<DatastreamTask> tasks) {
    LOG.info("onAssignmentChange called with datastream tasks {}", tasks);
    Set<DatastreamTask> unassigned = new HashSet<>(_fileProcessors.keySet());
    unassigned.removeAll(tasks);

    // Stop any processors for unassigned tasks
    stopProcessorForTasks(unassigned);

    for (DatastreamTask task : tasks) {
      if (!_fileProcessors.containsKey(task)) {
        try {
          LOG.info("Creating file processor for " + task);
          FileProcessor processor = new FileProcessor(task, task.getEventProducer());
          _fileProcessors.put(task, processor);
          _executorService.submit(processor);
        } catch (FileNotFoundException e) {
          // TODO This is not good, How do i handle exceptions here?
          throw new RuntimeException("FileProcessor threw an exception", e);
        }
      }
    }
  }

  @Override
  public void initializeDatastream(Datastream stream, List<Datastream> allDatastreams)
      throws DatastreamValidationException {
    LOG.info("validating datastream " + stream.toString());
    File streamFile = new File(stream.getSource().getConnectionString());
    if (!streamFile.exists() || !streamFile.isFile()) {
      throw new DatastreamValidationException(String.format("File %s doesn't exists", streamFile.getAbsolutePath()));
    }

    if (_numPartitions != 1) {
      stream.getSource().setPartitions(_numPartitions);
    }
  }
}
