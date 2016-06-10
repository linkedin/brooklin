package com.linkedin.datastream.connectors.mysql;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.connectors.mysql.or.InMemoryTableInfoProvider;
import com.linkedin.datastream.connectors.mysql.or.MysqlBinlogEventListener;
import com.linkedin.datastream.connectors.mysql.or.MysqlBinlogParser;
import com.linkedin.datastream.connectors.mysql.or.MysqlBinlogParserListener;
import com.linkedin.datastream.connectors.mysql.or.MysqlQueryUtils;
import com.linkedin.datastream.connectors.mysql.or.MysqlReplicator;
import com.linkedin.datastream.connectors.mysql.or.MysqlReplicatorImpl;
import com.linkedin.datastream.connectors.mysql.or.MysqlServerTableInfoProvider;
import com.linkedin.datastream.connectors.mysql.or.MysqlSourceBinlogRowEventFilter;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;


public class MysqlConnector implements Connector {

  public enum SourceType {
    // Use binlog folder in the filesystem as a source for the connector to ingest events. This is used for testing.
    FILESYSTEM,

    // Use binlogs from Mysql server as the source for the connector to ingest events.
    MYSQLSERVER
  }

  private static final Logger LOG = LoggerFactory.getLogger(MysqlConnector.class);
  public static final String CFG_MYSQL_USERNAME = "username";
  public static final String CFG_MYSQL_PASSWORD = "password";
  public static final String CFG_MYSQL_SERVER_ID = "serverId";
  public static final String CFG_MYSQL_SOURCE_TYPE = "sourceType";
  public static final String DEFAULT_MYSQL_SOURCE_TYPE = SourceType.MYSQLSERVER.toString();

  private static final int SHUTDOWN_TIMEOUT_MS = 5000;
  private static final int PRODUCER_STOP_TIMEOUT_MS = 5000;

  private final String _defaultUserName;
  private final String _defaultPassword;
  private final int _defaultServerId;
  private final SourceType _sourceType;

  private ConcurrentHashMap<DatastreamTask, MysqlReplicator> _mysqlProducers;

  private final Gauge<Integer> _numDatastreamTasks;
  private int _numTasks = 0;

  public MysqlConnector(Properties config) throws DatastreamException {
    _defaultUserName = config.getProperty(CFG_MYSQL_USERNAME);
    _defaultPassword = config.getProperty(CFG_MYSQL_PASSWORD);
    String strServerId = config.getProperty(CFG_MYSQL_SERVER_ID);
    // TODO we should pick up the username and password from the datastream rather than the defaults.
    if (_defaultUserName == null || _defaultPassword == null || strServerId == null) {
      throw new DatastreamException("Missing mysql username, password, or serverId property.");
    }
    _sourceType = SourceType.valueOf(config.getProperty(CFG_MYSQL_SOURCE_TYPE, DEFAULT_MYSQL_SOURCE_TYPE));
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

  private MysqlCheckpoint fetchMysqlCheckpoint(DatastreamTask task) {
    if (task.getCheckpoints() == null || task.getCheckpoints().get(0) == null) {
      return null;
    }
    return new MysqlCheckpoint(task.getCheckpoints().get(0));
  }

  private MysqlReplicator createReplicator(DatastreamTask task) {
    if (_sourceType == SourceType.FILESYSTEM) {
      return createReplicatorForBinLog(task);
    } else {
      return createReplicatorForMysqlServer(task);
    }
  }

  private MysqlReplicator createReplicatorForBinLog(DatastreamTask task) {
    MysqlSource source = parseMysqlSource(task);
    String binlogFolder;
    try {
      // We use the hostname part of the URI to describe the binlogFolder if the source type is filesystem.
      binlogFolder = URLDecoder.decode(source.getHostName(), StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      String msg = "Decoding the url threw an exception";
      LOG.error(msg, e);
      throw new DatastreamRuntimeException(msg, e);
    }

    MysqlSourceBinlogRowEventFilter rowEventFilter =
        new MysqlSourceBinlogRowEventFilter(source.getDatabaseName(), source.getTableName());
    MysqlBinlogParser replicator = new MysqlBinlogParser(binlogFolder, rowEventFilter, new MysqlBinlogParserListener());
    replicator.setBinlogEventListener(
        new MysqlBinlogEventListener(task, InMemoryTableInfoProvider.getTableInfoProvider()));
    return replicator;
  }

  private MysqlReplicatorImpl createReplicatorForMysqlServer(DatastreamTask task) {
    MysqlSource source = parseMysqlSource(task);

    MysqlSourceBinlogRowEventFilter rowEventFilter =
        new MysqlSourceBinlogRowEventFilter(source.getDatabaseName(), source.getTableName());
    MysqlBinlogParserListener parserListener = new MysqlBinlogParserListener();

    MysqlReplicatorImpl replicator =
        new MysqlReplicatorImpl(source, _defaultUserName, _defaultPassword, _defaultServerId, rowEventFilter,
            parserListener);

    MysqlServerTableInfoProvider tableInfoProvider = null;
    try {
      tableInfoProvider = new MysqlServerTableInfoProvider(source, _defaultUserName, _defaultPassword);
    } catch (SQLException e) {
      String msg = String.format("Unable to instantiate the table info provider for the source {%s}", source);
      LOG.error(msg, e);
      throw new DatastreamRuntimeException(msg, e);
    }

    replicator.setBinlogEventListener(new MysqlBinlogEventListener(task, tableInfoProvider));
    _mysqlProducers.put(task, replicator);

    MysqlQueryUtils queryUtils = new MysqlQueryUtils(source, _defaultUserName, _defaultPassword);

    MysqlCheckpoint checkpoint = fetchMysqlCheckpoint(task);
    String binlogFileName;
    long position;
    if (checkpoint == null) {
      // start from the latest position if no checkpoint found
      MysqlQueryUtils.BinlogPosition binlogPosition;
      try {
        binlogPosition = queryUtils.getLatestLogPositionOnMaster();
      } catch (SQLException e) {
        String msg = String.format("Unable to get the latest log position on the master {%s}", source);
        LOG.error(msg, e);
        throw new DatastreamRuntimeException(msg, e);
      }
      binlogFileName = binlogPosition.getFileName();
      position = binlogPosition.getPosition();
    } else {
      binlogFileName = checkpoint.getBinlogFileName();
      position = checkpoint.getBinlogOffset();
    }
    replicator.setBinlogFileName(binlogFileName);
    replicator.setBinlogPosition(position);

    return replicator;
  }

  private MysqlSource parseMysqlSource(DatastreamTask task) {
    String sourceConnectionString = task.getDatastreamSource().getConnectionString();
    try {
      return MysqlSource.createFromUri(sourceConnectionString);
    } catch (SourceNotValidException e) {
      String msg = String.format("Error while parsing the mysql source %s", sourceConnectionString);
      LOG.error(msg, e);
      throw new DatastreamRuntimeException(msg, e);
    }
  }

  @Override
  public synchronized void onAssignmentChange(List<DatastreamTask> tasks) {
    if (Optional.ofNullable(tasks).isPresent()) {
      LOG.info(String.format("onAssignmentChange called with datastream tasks %s ", tasks.toString()));
      _numTasks = tasks.size();
      for (DatastreamTask task : tasks) {
        MysqlReplicator replicator = _mysqlProducers.get(task);
        if (replicator == null) {
          LOG.info("New task {%s} assigned to the current instance, Creating mysql producer for " + task.toString());
          replicator = createReplicator(task);
        }

        startReplicator(replicator);
      }

      stopReassignedTasks(tasks);
    }
  }

  private void startReplicator(MysqlReplicator replicator) {
    try {
      replicator.start();
    } catch (Exception e) {
      String msg = String.format("Starting replicator {%s} failed with exception", replicator);
      LOG.error(msg, e);
      throw new DatastreamRuntimeException(msg, e);
    }
  }

  private void stopReassignedTasks(List<DatastreamTask> tasks) {
    List<DatastreamTask> reassignedTasks =
        _mysqlProducers.keySet().stream().filter(tasks::contains).collect(Collectors.toList());

    if (!reassignedTasks.isEmpty()) {
      LOG.info("Tasks {%s} are reassigned from the current instance, Stopping the replicators", reassignedTasks);
      for (DatastreamTask reassignedTask : reassignedTasks) {
        MysqlReplicator replicator = _mysqlProducers.get(reassignedTask);
        try {
          replicator.stop(PRODUCER_STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
          LOG.warn(String.format("Stopping the replicator {%s} for task {%s} failed with exception", replicator,
              reassignedTask), e);
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
