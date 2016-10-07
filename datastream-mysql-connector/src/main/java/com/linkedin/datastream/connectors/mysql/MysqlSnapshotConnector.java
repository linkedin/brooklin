package com.linkedin.datastream.connectors.mysql;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.connectors.mysql.or.InMemoryTableInfoProvider;
import com.linkedin.datastream.connectors.mysql.or.MysqlQueryUtils;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;


/**
 * Mysql snapshot connector that reads the current snapshot of the entire Mysql table and writes to the
 * transport provider.
 *
 * If the datastream gets rebalanced in during the process in between. Then the new datastream instance that gets the
 * datastream reassigned will start the snapshot write process from the beginning. This is done because there is
 * no way to get to the right position of the older snapshot without disabling any writes on the table or db.
 */
public class MysqlSnapshotConnector implements Connector {

  private static final Logger LOG = LoggerFactory.getLogger(MysqlSnapshotConnector.class);

  public static final String CONNECTOR_NAME = "mysqlsnapshot";
  private static final String CFG_NUM_THREADS = "numThreads";
  private static final String DEFAULT_NUM_THREADS = "10";
  private static final String CFG_BUFFER_SIZE = "bufferSize";
  private static final String DEFAULT_BUFFER_SIZE = "100";
  private static final String USE_IN_MEMORY_TABLE_INFO_PROVIDER = "useInMemoryTableInfoProvider";
  private static final String DEFAULT_SQL_TIMEOUT = "5";
  private static final String CFG_SQL_TIMEOUT = "sqlTimeout";
  private static final Duration DEFAULT_STOP_TIMEOUT = Duration.ofMinutes(1);

  private final String _defaultUserName;
  private final String _defaultPassword;
  private final ExecutorService _executorService;
  private final int _bufferSizeInRows;
  private final Duration _sqlTimeout;
  private InMemoryTableInfoProvider _tableInfoProvider = null;
  private Map<DatastreamTask, SnapshotTaskHandler> _taskHandlers = new HashMap<>();

  public MysqlSnapshotConnector(Properties config) {
    _defaultUserName = config.getProperty(MysqlConnector.CFG_MYSQL_USERNAME, "");
    _defaultPassword = config.getProperty(MysqlConnector.CFG_MYSQL_PASSWORD, "");
    _bufferSizeInRows = Integer.parseInt(config.getProperty(CFG_BUFFER_SIZE, DEFAULT_BUFFER_SIZE));

    if (Boolean.parseBoolean(config.getProperty(USE_IN_MEMORY_TABLE_INFO_PROVIDER, "false"))) {
      _tableInfoProvider = InMemoryTableInfoProvider.getTableInfoProvider();
    }

    _sqlTimeout = Duration.ofMillis(Integer.parseInt(config.getProperty(CFG_SQL_TIMEOUT, DEFAULT_SQL_TIMEOUT)));
    int numberOfThreads = Integer.parseInt(config.getProperty(CFG_NUM_THREADS, DEFAULT_NUM_THREADS));
    _executorService = Executors.newFixedThreadPool(numberOfThreads);
  }

  @Override
  public void start() {
    LOG.info("Start called.");
  }

  @Override
  public void stop() {
    stopAndRemoveHandlers(_taskHandlers.keySet());
    try {
      _executorService.awaitTermination(DEFAULT_STOP_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for snapshot tasks to complete.", e);
    }

    LOG.info("Mysql snapshot connector is stopped.");
  }

  @Override
  public void onAssignmentChange(List<DatastreamTask> tasks) {
    LOG.info(String.format("Mysql snapshot connector is assigned tasks %s", tasks));
    List<DatastreamTask> tasksAdded =
        tasks.stream().filter(x -> !_taskHandlers.containsKey(x)).collect(Collectors.toList());
    List<DatastreamTask> tasksRemoved =
        _taskHandlers.keySet().stream().filter(x -> !tasks.contains(x)).collect(Collectors.toList());

    // Stop the handlers for the removed tasks.
    stopAndRemoveHandlers(tasksRemoved);

    // Start the handlers for the added tasks.
    Map<DatastreamTask, SnapshotTaskHandler> addedHandlers = tasksAdded.stream()
        .collect(Collectors.toMap(Function.identity(),
            x -> new SnapshotTaskHandler(x, _tableInfoProvider, getUsername(x), getPassword(x), _bufferSizeInRows,
                _sqlTimeout)));

    addedHandlers.values().stream().forEach(_executorService::submit);

    _taskHandlers.putAll(addedHandlers);
  }

  private String getUsername(DatastreamTask task) {
    return getUserNameFromDatastream(task.getDatastreams().get(0));
  }

  private String getPassword(DatastreamTask task) {
    return getPasswordFromDatastream(task.getDatastreams().get(0));
  }

  private String getUserNameFromDatastream(Datastream datastream) {
    return datastream.hasMetadata() ? datastream.getMetadata()
        .getOrDefault(MysqlConnector.CFG_MYSQL_USERNAME, _defaultUserName) : _defaultUserName;
  }

  private String getPasswordFromDatastream(Datastream datastream) {
    return datastream.hasMetadata() ? datastream.getMetadata()
        .getOrDefault(MysqlConnector.CFG_MYSQL_PASSWORD, _defaultPassword) : _defaultPassword;
  }

  private void stopAndRemoveHandlers(Collection<DatastreamTask> tasksRemoved) {
    tasksRemoved.stream().map(_taskHandlers::get).forEach(SnapshotTaskHandler::stop);
    tasksRemoved.stream().forEach(_taskHandlers::remove);
  }

  @Override
  public void initializeDatastream(Datastream stream, List<Datastream> allDatastreams)
      throws DatastreamValidationException {
    LOG.info("validating datastream " + stream.toString());
    try {
      MysqlSource source = MysqlSource.createFromUri(stream.getSource().getConnectionString());
      if (source.getSourceType() != MysqlConnector.SourceType.MYSQLSERVER) {
        String msg = "Connector can only support datastreams of MYSQLSERVER source type";
        LOG.error(msg);
        throw new DatastreamValidationException(msg);
      }

      if (!MysqlQueryUtils.checkTableExists(source, getUserNameFromDatastream(stream),
          getPasswordFromDatastream(stream))) {
        throw new DatastreamValidationException(
            String.format("Invalid datastream: Can't find table %s in db %s.", source.getDatabaseName(),
                source.getTableName()));
      }

      if (stream.getSource().hasPartitions() && stream.getSource().getPartitions() != 1) {
        String msg = "Mysql snapshot connector can only support single partition sources";
        LOG.error(msg);
        throw new DatastreamValidationException(msg);
      }
    } catch (SourceNotValidException | SQLException e) {
      LOG.error("Received exception while validating the Mysql source", e);
      throw new DatastreamValidationException(e);
    }
  }

  @Override
  public List<BrooklinMetricInfo> getMetricInfos() {
    return Collections.emptyList();
  }
}
