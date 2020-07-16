/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.ShutdownTaskHandler;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.providers.CheckpointProvider;


/**
 * This is a common layer for all the connectors.
 */
public abstract class AbstractConnector implements Connector {

  protected final ScheduledExecutorService _daemonThreadExecutorService;
  public final ShutdownTaskHandler _shutdownTaskHandler;
  protected final String _connectorName;
  protected final Logger _logger;

  private int _daemonThreadIntervalInSecond = 300;

  /**
   *
   * @param connectorName Name of the connector
   * @param shutdownExecutorShutdownTimeout timout to shutdown shutDownExecutorService
   * @param logger logger
   */
  public AbstractConnector(String connectorName, Duration shutdownExecutorShutdownTimeout, Logger logger) {
    _connectorName = connectorName;
    _shutdownTaskHandler = new ShutdownTaskHandler(shutdownExecutorShutdownTimeout);
    _logger = logger;
    // A daemon executor to constantly check whether all tasks are running and restart them if not.
    _daemonThreadExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
          @Override
          public Thread newThread(@NotNull Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName(String.format("%s daemon thread", _connectorName));
            return t;
          }
        });
  }

  /**
   * Method to start the connector.
   * This is called immediately after the connector is instantiated. This typically happens when brooklin server starts up.
   * @param checkpointProvider CheckpointProvider if the connector needs a checkpoint store.
   */
  @Override
  public void start(CheckpointProvider checkpointProvider) {
    _shutdownTaskHandler.start();
    _daemonThreadExecutorService.scheduleAtFixedRate(() -> {
      try {
        restartDeadTasks();
      } catch (Exception e) {
        // catch any exceptions here so that subsequent check can continue
        // see java doc of scheduleAtFixedRate
        _logger.warn("Failed to check status of connector tasks.", e);
      }
      }, getDaemonThreadDelayTimeInSecond(_daemonThreadIntervalInSecond), _daemonThreadIntervalInSecond, TimeUnit.SECONDS);
  }

  /**
   * Method to stop the connector. This is called when the brooklin server is being stopped.
   */
  @Override
  public void stop() {
    _daemonThreadExecutorService.shutdown();
    _shutdownTaskHandler.stop();
  }

  /**
   * Callback when the datastreams assigned to this instance is changed. The implementation of the Connector is
   * responsible to keep a state of the previous assignment.
   * @param tasks the list of the current assignment.
   */
  @Override
  public void onAssignmentChange(List<DatastreamTask> tasks) {

  }

  /**
   * Initialize the datastream. Any connector-specific validations for the datastream needs to performed here.
   * Connector can mutate the datastream object in this call. Framework will write the updated datastream object.
   * NOTE:
   * <ol>
   *   <li>
   *     This method is called by the Rest.li service before the datastream is written to ZooKeeper, so please make sure,
   *     this call doesn't block for more then few seconds otherwise the REST call will time out.</li>
   *   <li>
   *     It is possible that the brooklin framework may call this method in parallel to another onAssignmentChange
   *     call. It is up to the connector to perform the synchronization if it needs between initialize and onAssignmentChange.</li>
   * </ol>
   * @param stream Datastream model
   * @param allDatastreams all existing datastreams in the system of connector type of the datastream that is being
   *                       initialized.
   * @throws DatastreamValidationException when the datastream that is being created fails any validation.
   */
  @Override
  public void initializeDatastream(Datastream stream, List<Datastream> allDatastreams)
      throws DatastreamValidationException {

  }

  /**
   * @param task the task to submitShutdownTask
   * @param <T> the type of the task's result
   * @return a Future representing pending completion of the task
   */
  public <T> Future<T> submitShutdownTask(Callable<T> task) {
    return _shutdownTaskHandler.submit(task);
  }

  protected abstract void restartDeadTasks();

  protected long getDaemonThreadDelayTimeInSecond(int daemonThreadIntervalInSecond) {
    return daemonThreadIntervalInSecond;
  }

  protected void setDaemonThreadIntervalInSecond(int daemonThreadIntervalInSecond) {
    _daemonThreadIntervalInSecond = daemonThreadIntervalInSecond;
  }

}

