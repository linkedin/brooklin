package com.linkedin.datastream.server;

import com.linkedin.datastream.common.Datastream;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 */
public class EspressoBootstrapConnector implements Connector {
  // TODO: make this config options
  private static final long TOPIC_RETENTION = 172800000;

  private static final double TOPIC_REUSE_THRESHOLD = 0.5;

  private static final int NUM_CONCURRENT_PRODUCERS = 10;

  private static final String CONNECTOR_TYPE = "ESPRESSO_BOOTSTRAP";

  private static Logger LOG = LoggerFactory.getLogger(EspressoBootstrapConnector.class);

  private ExecutorService _executorService;

  static class Target implements DataStreamTarget {
    private static final int TOPIC_PARTS = 5;
    private static final int SOURCE_PARTS = 4;
    private static final String TOPIC_FORMAT = "ESPRESSO_BOOTSTRAP_%s_%s_%s_%d_%d";

    private String _fabric;
    private String _database;
    private String _table;
    private int _partitions;
    private long _timestamp;

    private String _topicName;
    private boolean _reusable;

    private void parseSource(String source) {
      String[] parts = source.split("_");
      if (parts.length != SOURCE_PARTS)
        throw new IllegalArgumentException("Invalid source: " + source);

      _fabric = parts[0];
      _database = parts[1];
      _table = parts[2];
      _partitions = Integer.valueOf(parts[3]);
      _timestamp = Calendar.getInstance().getTimeInMillis();

      _topicName = String.format(TOPIC_FORMAT, _fabric,
              _database, _table, _partitions, _timestamp);

      _reusable = false;
    }

    private void parseExisting(String topicName) {
      String[] parts = topicName.split("_");
      if (parts.length != TOPIC_PARTS)
        throw new IllegalArgumentException("Invalid topic: " + topicName);

      _fabric = parts[0];
      _database = parts[1];
      _table = parts[2];
      _partitions = Integer.valueOf(parts[3]);
      _timestamp = Long.valueOf(parts[4]);

      _topicName = topicName;

      // Exceed the reusable threshold?
      double elapsed = Calendar.getInstance().getTimeInMillis() - _timestamp;
      _reusable = ((elapsed / TOPIC_RETENTION) < TOPIC_REUSE_THRESHOLD);
    }

    public Target(Datastream stream) {
      String currentTopic = stream.getTarget().getKafkaConnection().getTopicName();
      if (currentTopic == null || currentTopic.equals("")) {
        parseSource(stream.getSource());
      } else {
        parseExisting(currentTopic);
      }
    }

    @Override
    public String getTargetName() {
      return _topicName;
    }

    @Override
    public int getNumPartitions() {
      return _partitions;
    }

    @Override
    public boolean isReusable() {
      return _reusable;
    }
  }

  class Producer implements Runnable {
    private final Datastream _datastream;
    private final Target _target;
    private final DataStreamWritableChannel<String, GenericRecord> _channel;

    public Producer(Datastream datastream, Target target,
      DataStreamWritableChannel<String, GenericRecord> channel) {
      _datastream = datastream;
      _target = target;
      _channel = channel;
    }

    @Override
    public void run() {
      // TODO: write into Kafka topic
      // snapshot = readSnapshot(_target);
      // for(row: snapshot) {
      //  record = createRecord(row);
      //  _channel.write(record);
      // }
    }
  }

  @Override
  public void start() {
    _executorService = Executors.newFixedThreadPool(NUM_CONCURRENT_PRODUCERS);
    LOG.debug("started.");
  }

  @Override
  public void stop() {
    // TODO: shutdownNow()?
    _executorService.shutdown();
    LOG.debug("shutdown.");
  }

  @Override
  public String getConnectorType() {
    return CONNECTOR_TYPE;
  }

  @Override
  public DataStreamTarget getTarget(Datastream stream) {
    return new Target(stream);
  }

  private void assign(DatastreamContext context, DatastreamTask task) {
    task.getDatastreams().stream().forEach((stream) -> {
      Target target = new Target(stream);
      if (!target.isReusable()) {
        DataStreamWritableChannel<String, GenericRecord> channel =
                DataStreamChannelFactory.createChannel(target);
        _executorService.submit(new Producer(stream, target, channel));
      }
    });
  }

  @Override
  public void onAssignmentChange(DatastreamContext context, List<DatastreamTask> tasks) {
    /**
    Before being called, coordinator is supposed to have created the topic
    with help from our getTarget() such that Connector can simply create a
    channel around it.
     */

    // Assuming this is a new assignment for now
    tasks.stream().forEach((task) -> assign(context, task));

    // NOTE: bootstrap connector ignores any unassignments
    // In other words, assignment strategy shouldn't unassign
    // a partition unless the connector died. Then it's a new
    // assignment for some other Connector instance.
  }
}
