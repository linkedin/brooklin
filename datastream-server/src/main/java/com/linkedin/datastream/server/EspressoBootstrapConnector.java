package com.linkedin.datastream.server;

import com.linkedin.datastream.common.Datastream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Bootstrap connector implementatin for Espresso database.
 */
public class EspressoBootstrapConnector implements Connector {
  // TODO: make below values config options
  private static final long TOPIC_RETENTION = 172800000;
  private static final double TOPIC_REUSE_THRESHOLD = 0.5;
  private static final int NUM_CONCURRENT_PRODUCERS = 10;

  private static Logger LOG = LoggerFactory.getLogger(EspressoBootstrapConnector.class);
  private static final String CONNECTOR_TYPE = "ESPRESSO_BOOTSTRAP";

  private ExecutorService _executorService;
  private Map<String, AtomicBoolean> _stopRequested;

  static class Target implements DatastreamTarget {
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

  static class Producer implements Runnable {
    private final Target _target;
    private final Datastream _datastream;
    private final DatastreamWritableChannel _channel;
    private final AtomicBoolean _stopSignal;
    private final DatastreamContext _context;

    public Producer(Datastream datastream, DatastreamWritableChannel channel,
      Target target, AtomicBoolean stopSignal, DatastreamContext context) {
      _datastream = datastream;
      _target = target;
      _channel = channel;
      _stopSignal = stopSignal;
      _context = context;
    }

    @Override
    public void run() {
      // TODO: write into Kafka topic
      while (!_stopSignal.get()) {
        // snapshot = readSnapshot(_target);
        // for(row: snapshot) {
        //  record = createRecord(row);
        //  _channel.write(record);
        // }
      }

      // TODO: save snapshot offset etc in Context
    }
  }

  @Override
  public void start() {
    _executorService = Executors.newFixedThreadPool(NUM_CONCURRENT_PRODUCERS);
    _stopRequested = new HashMap();
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
  public DatastreamTarget getDatastreamTarget(Datastream stream) {
    return new Target(stream);
  }

  private void assign(DatastreamContext context, DatastreamTask task) {
    Set<String> seen = new HashSet<String>();
    task.getDatastreamChannels().stream().forEach((channel) -> {
      Target target = new Target(channel.getDatastream());
      seen.add(target.getTargetName());
      if (_stopRequested.containsKey(target.getTargetName()))
        return;
      if (!target.isReusable()) {
        AtomicBoolean signal = new AtomicBoolean(false);
        Producer producer = new Producer(channel.getDatastream(), channel.getChannel(), target, signal, context);
        _stopRequested.put(target.getTargetName(), signal);
        _executorService.submit(producer);
      }
    });

    // TODO: simplify the stop logic

    // Notify producers to stop
    for (String key: _stopRequested.keySet()) {
      if (!seen.contains(key)) {
        _stopRequested.get(key).set(true);
      }
    }

    // Remove stopped producers
    for (String key: _stopRequested.keySet()) {
      if (_stopRequested.get(key).get())
        _stopRequested.remove(key);
    }
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
