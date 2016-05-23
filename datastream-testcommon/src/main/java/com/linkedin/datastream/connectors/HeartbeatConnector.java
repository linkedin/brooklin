package com.linkedin.datastream.connectors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Metric;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamEvent;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.DatastreamProducerRecordBuilder;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;


/**
 * Hearbeat connector sends heartbeat events for each of the datastream tasks at a regular interval.
 * This connector is mainly used for both automated and manual testing.
 */
public class HeartbeatConnector implements Connector {
  private static final Logger LOG = LoggerFactory.getLogger(HeartbeatConnector.class);

  /**
   * Default timeperiod at which the heartbeat connector sends events.
   */
  private static final String DEFAULT_HEARTBEAT_PERIOD_MS = "5000";
  private static final String CFG_HEARTBEAT_PERIOD = "heartbeatPeriodMs";

  private static final String BROADCAST_CONNECTOR_TYPE = "hearbeatbc";
  private static final String LOADBALANCING_CONNECTOR_TYPE = "hearbeatlb";

  private final int _hearbeatPeriodMs;

  private ScheduledThreadPoolExecutor _executor;
  private List<DatastreamTask> _tasks;

  private Map<DatastreamTask, Map<Integer, Integer>> _checkpoint = new HashMap<>();

  public HeartbeatConnector(Properties config) {
    LOG.info(String.format("Creating Heartbeat connector with config: %s", config));
    _tasks = new ArrayList<>();
    _hearbeatPeriodMs = Integer.parseInt(config.getProperty(CFG_HEARTBEAT_PERIOD, DEFAULT_HEARTBEAT_PERIOD_MS));
  }

  @Override
  public void start() {
    _executor = new ScheduledThreadPoolExecutor(1);
    _executor.scheduleAtFixedRate(this::sendHeartBeatEvents, 0, _hearbeatPeriodMs, TimeUnit.MILLISECONDS);

  }

  private void sendHeartBeatEvents() {
    LOG.info(String.format("Sending heartbeat event for tasks: %s", _tasks));
    try {
      DatastreamTask[] tasks = new DatastreamTask[_tasks.size()];
      _tasks.toArray(tasks);
      for (DatastreamTask task : tasks) {
        if (task.getConnectorType().equalsIgnoreCase(BROADCAST_CONNECTOR_TYPE)) {
          Integer eventIndex = _checkpoint.get(task).get(0);
          task.getEventProducer().send(createHeartbeatEvent(0, eventIndex), null);
          _checkpoint.get(task).put(0, eventIndex + 1);
        } else {
          for (int partition : task.getPartitions()) {
            Integer eventIndex = _checkpoint.get(task).get(partition);
            task.getEventProducer().send(createHeartbeatEvent(partition, eventIndex), null);
            _checkpoint.get(task).put(partition, eventIndex + 1);
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Exception while sending the event", e);
    }
  }

  private DatastreamProducerRecord createHeartbeatEvent(int partition, int eventIndex) {
    DatastreamEvent event = new DatastreamEvent();
    event.key = ByteBuffer.wrap(Integer.toString(eventIndex).getBytes());
    event.payload = ByteBuffer.wrap("Heartbeat".getBytes());
    DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
    builder.addEvent(event);
    builder.setPartition(partition);
    builder.setSourceCheckpoint(Integer.toString(eventIndex));
    return builder.build();
  }

  @Override
  public void stop() {
    _executor.shutdown();
  }

  @Override
  public void onAssignmentChange(List<DatastreamTask> tasks) {
    _tasks = tasks;
    for (DatastreamTask task : tasks) {
      if (!_checkpoint.containsKey(task)) {
        _checkpoint.put(task, new HashMap<>());
        Map<Integer, String> taskCheckpoint = task.getCheckpoints();
        LOG.info(String.format("Assigned a new Datastream task %s with checkpoint %s", task, taskCheckpoint));
        for (Integer partition : taskCheckpoint.keySet()) {
          int eventIndex = 0;
          if (!taskCheckpoint.get(partition).isEmpty()) {
             eventIndex = Integer.parseInt(taskCheckpoint.get(partition));
          }
          _checkpoint.get(task).put(partition, eventIndex + 1);
        }
      }
    }
  }

  @Override
  public void initializeDatastream(Datastream stream, List<Datastream> allDatastreams)
      throws DatastreamValidationException {
  }

  @Override
  public Map<String, Metric> getMetrics() {
    return null;
  }
}
