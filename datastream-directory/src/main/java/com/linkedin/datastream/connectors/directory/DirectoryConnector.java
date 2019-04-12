/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.directory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.providers.CheckpointProvider;


/**
 * A {@link Connector} implementation intended for consuming and propagating
 * events about changes occurring in a directory in the file system.
 * <br/>
 * <strong>This connector is for demonstration purposes only.</strong>
 */
public class DirectoryConnector implements Connector {
  public static final String CONNECTOR_NAME = "directory";

  private static final Logger LOG = LoggerFactory.getLogger(DirectoryConnector.class);

  private final ConcurrentMap<DatastreamTask, DirectoryChangeProcessor> _directoryProcessors;
  private final ExecutorService _executorService;

  /**
   * Constructor for DirectoryConnector
   * @param threadPoolSize Number of threads in the thread pool used
   *                       for watching and processing directory changes.
   */
  public DirectoryConnector(int threadPoolSize) {
    Validate.isTrue(threadPoolSize > 0, "Thread pool size must be greater than zero");

    _directoryProcessors = new ConcurrentHashMap<>();
    _executorService = Executors.newFixedThreadPool(threadPoolSize);
  }

  @Override
  public void start(CheckpointProvider checkpointProvider) {
    LOG.info("DirectoryConnector started");
  }

  @Override
  public void stop() {
    stopTaskProcessors(_directoryProcessors.keySet());
  }

  @Override
  public void onAssignmentChange(List<DatastreamTask> tasks) {
    Validate.notNull(tasks);

    LOG.info("onAssignmentChange called with datastream tasks {}", tasks);
    Set<DatastreamTask> existingTasks = _directoryProcessors.keySet();
    Set<DatastreamTask> newTasks = new HashSet<>(tasks);

    Iterable<DatastreamTask> stopTasks = Sets.difference(existingTasks, newTasks);
    stopTaskProcessors(stopTasks);

    Iterable<DatastreamTask> startTasks = Sets.difference(newTasks, existingTasks);
    startTaskProcessors(startTasks);
  }

  @Override
  public void initializeDatastream(Datastream stream, List<Datastream> allDatastreams)
      throws DatastreamValidationException {
    Validate.notNull(stream);
    Validate.notNull(allDatastreams);

    LOG.info("validating datastream " + stream.toString());

    String sourceDirectoryPath = stream.getSource().getConnectionString();
    if (!DirectoryChangeProcessor.isDirectory(sourceDirectoryPath)) {
      throw new DatastreamValidationException(String.format("Path %s is not a directory",
          sourceDirectoryPath));
    }

    // single partition datastream
    stream.getSource().setPartitions(1);
  }

  private void startTaskProcessors(Iterable<DatastreamTask> datastreamTasks) {
    for (DatastreamTask datastreamTask : datastreamTasks) {
      try {
        DirectoryChangeProcessor processor = new DirectoryChangeProcessor(datastreamTask, datastreamTask.getEventProducer());
        _directoryProcessors.put(datastreamTask, processor);
        _executorService.submit(processor);
      } catch (IOException ex) {
        LOG.error("Encountered an error while attempting to process {}", datastreamTask);
      }
    }
  }

  private void stopTaskProcessors(Iterable<DatastreamTask> datastreamTasks) {
    for (DatastreamTask datastreamTask : datastreamTasks) {
      DirectoryChangeProcessor processor = _directoryProcessors.remove(datastreamTask);
      if (processor != null) {
        processor.close();
      }
      LOG.info("Processor stopped for task: " + datastreamTask);
    }
  }
}
