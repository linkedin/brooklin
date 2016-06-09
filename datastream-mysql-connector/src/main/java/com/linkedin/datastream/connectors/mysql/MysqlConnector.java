package com.linkedin.datastream.connectors.mysql;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.connectors.mysql.or.MysqlBinlogEventListener;
import com.linkedin.datastream.connectors.mysql.or.MysqlBinlogParserListener;
import com.linkedin.datastream.connectors.mysql.or.MysqlQueryUtils;
import com.linkedin.datastream.connectors.mysql.or.MysqlReplicator;
import com.linkedin.datastream.connectors.mysql.or.MysqlSourceBinlogRowEventFilter;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;


public class MysqlConnector implements Connector {
  private static final Logger LOG = LoggerFactory.getLogger(MysqlConnector.class);
  public static final String CFG_MYSQL_USERNAME = "username";
  public static final String CFG_MYSQL_PASSWORD = "password";
  public static final String CFG_MYSQL_SERVERID = "serverid";

  private static final int SHUTDOWN_TIMEOUT_MS = 5000;

  private final String _defaultUserName;
  private final String _defaultPassword;
  private final int _defaultServerId;
  private ConcurrentHashMap<DatastreamTask, MysqlReplicator> _mysqlProducers;

  private final Gauge<Integer> _numDatastreamTasks;
  private int _numTasks = 0;

  public MysqlConnector(Properties config) throws DatastreamException {
    _defaultUserName = config.getProperty(CFG_MYSQL_USERNAME);
    _defaultPassword = config.getProperty(CFG_MYSQL_PASSWORD);
    String strServerId = config.getProperty(CFG_MYSQL_SERVERID);
    if (_defaultUserName == null || _defaultPassword == null || strServerId == null) {
      throw new DatastreamException("Missing mysql username, password, or serverid property.");
    }
    _defaultServerId = Integer.valueOf(strServerId);
    _mysqlProducers = new ConcurrentHashMap<>();

    // initialize metrics
    _numDatastreamTasks = () -> _numTasks;
  }

  @Override
  public void start() {
  }

  @Override
  public synchronized void stop() {
    _mysqlProducers.keySet().stream().forEach(task -> {
      MysqlReplicator producer = _mysqlProducers.get(task);
      if (producer.isRunning()) {
        try {
          producer.stop(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
          LOG.error("Stopping the producer failed with error " + e.toString());
        }
      }
    });
    LOG.info("stopped.");
  }

  private MysqlCheckpoint getMysqlCheckpoint(DatastreamTask task) {
    if (task.getCheckpoints() == null || task.getCheckpoints().get(0) == null) {
      return null;
    }
    return new MysqlCheckpoint(task.getCheckpoints().get(0));
  }

  private MysqlReplicator createReplicator(DatastreamTask task) throws SQLException, SourceNotValidException {
    MysqlSource source = MysqlSource.createFromUri(task.getDatastreamSource().getConnectionString());

    MysqlSourceBinlogRowEventFilter rowEventFilter =
        new MysqlSourceBinlogRowEventFilter(source.getDatabaseName(), source.getTableName());
    MysqlBinlogParserListener parserListener = new MysqlBinlogParserListener();

    MysqlReplicator replicator =
        new MysqlReplicator(source, _defaultUserName, _defaultPassword, _defaultServerId, rowEventFilter,
            parserListener);
    replicator.setBinlogEventListener(new MysqlBinlogEventListener(source, task, _defaultUserName, _defaultPassword));
    _mysqlProducers.put(task, replicator);

    MysqlQueryUtils queryUtils = new MysqlQueryUtils(source, _defaultUserName, _defaultPassword);

    MysqlCheckpoint checkpoint = getMysqlCheckpoint(task);
    String binlogFileName;
    long position;
    if (checkpoint == null) {
      // start from the latest position if no checkpoint found
      MysqlQueryUtils.BinlogPosition binlogPosition = queryUtils.getLatestLogPositionOnMaster();
      binlogFileName = binlogPosition.getFileName();
      position = binlogPosition.getPosition();
    } else {
      // TODO maybe record the entire file name in MysqlCheckpoint?
      binlogFileName = String.format("mysql-bin.%06d", checkpoint.getBinlogFileNum());
      position = checkpoint.getBinlogOffset();
    }
    replicator.setBinlogFileName(binlogFileName);
    replicator.setBinlogPosition(position);

    return replicator;
  }

  @Override
  public synchronized void onAssignmentChange(List<DatastreamTask> tasks) {
    if (Optional.ofNullable(tasks).isPresent()) {
      LOG.info(String.format("onAssignmentChange called with datastream tasks %s ", tasks.toString()));
      _numTasks = tasks.size();
      for (DatastreamTask task : tasks) {
        if (!_mysqlProducers.contains(task)) {
          LOG.info("Creating mysql producer for " + task.toString());
          try {
            MysqlReplicator replicator = createReplicator(task);
            replicator.start();
          } catch (Exception e) {
            throw new RuntimeException("EventCollectorFactory threw an exception ", e);
          }
        }
      }
    }
  }

  private String getUserNameFromDatastream(Datastream datastream) {
    return datastream.hasMetadata() ? datastream.getMetadata().getOrDefault(CFG_MYSQL_USERNAME, _defaultUserName)
        : _defaultUserName;
  }

  private String getPasswordFromDatastream(Datastream datastream) {
    return datastream.hasMetadata() ? datastream.getMetadata().getOrDefault(CFG_MYSQL_PASSWORD, _defaultPassword)
        : _defaultPassword;
  }

  @Override
  public void initializeDatastream(Datastream stream, List<Datastream> allDatastreams)
      throws DatastreamValidationException {
    LOG.info("validating datastream " + stream.toString());
    try {
      MysqlSource source = MysqlSource.createFromUri(stream.getSource().getConnectionString());
      MysqlQueryUtils queryUtils =
          new MysqlQueryUtils(source, getUserNameFromDatastream(stream), getPasswordFromDatastream(stream));
      queryUtils.initializeConnection();
      if (!queryUtils.checkTableExist(source.getDatabaseName(), source.getTableName())) {
        throw new DatastreamValidationException(
            String.format("Invalid datastream: Can't find table %s in db %s.", source.getDatabaseName(),
                source.getTableName()));
      }
      queryUtils.closeConnection();
    } catch (SourceNotValidException | SQLException e) {
      throw new DatastreamValidationException(e);
    }
    if (stream.getSource().hasPartitions() && stream.getSource().getPartitions() != 1) {
      // we only allow partition number to be one in mysql
      stream.getSource().setPartitions(1);
    }
  }

  @Override
  public Map<String, Metric> getMetrics() {
    Map<String, Metric> metrics = new HashMap<>();

    metrics.put(buildMetricName("numDatastreamTasks"), _numDatastreamTasks);

    return Collections.unmodifiableMap(metrics);
  }
}
