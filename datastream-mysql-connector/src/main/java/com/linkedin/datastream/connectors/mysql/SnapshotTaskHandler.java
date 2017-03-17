package com.linkedin.datastream.connectors.mysql;

import java.sql.SQLException;
import java.time.Duration;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.RetriableException;
import com.linkedin.datastream.common.RetryUtils;
import com.linkedin.datastream.connectors.mysql.or.MysqlServerTableInfoProvider;
import com.linkedin.datastream.connectors.mysql.or.TableInfoProvider;
import com.linkedin.datastream.server.DatastreamEventProducer;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.DatastreamProducerRecordBuilder;
import com.linkedin.datastream.server.DatastreamTask;


/**
 * Snapshot task handler runs on a tight loop to read the data from mysql table one by one and writes it to the producer.
 */
public class SnapshotTaskHandler implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotTaskHandler.class);
  private static final Duration SQL_RETRY_PERIOD = Duration.ofMinutes(1);
  private final Duration _sqlTimeout;

  private final DatastreamTask _datastreamTask;
  private final String _password;
  private final String _username;
  private TableInfoProvider _tableInfoProvider;
  private final int _bufferSize;
  private final DatastreamEventProducer _eventProducer;
  private boolean _stopRequested = false;
  private boolean _stopped = false;

  public SnapshotTaskHandler(DatastreamTask datastreamTask, TableInfoProvider tableInfoProvider, String username,
      String password, int bufferSizeInRows, Duration sqlTimeout) {
    _datastreamTask = datastreamTask;
    _eventProducer = datastreamTask.getEventProducer();
    _bufferSize = bufferSizeInRows;
    _tableInfoProvider = tableInfoProvider;
    _sqlTimeout = sqlTimeout;
    _username = username;
    _password = password;
  }

  @Override
  public void run() {
    MysqlTableReader tableReader = createTableReaderWithRetries();
    BrooklinEnvelope event;
    while ((event = getNextEventWithRetries(tableReader)) != null) {
      sendEvent(event);
    }

    tableReader.close();
    _stopped = true;
  }

  private BrooklinEnvelope getNextEventWithRetries(MysqlTableReader tableReader) {

    return RetryUtils.retry(() -> {
      try {
        if (!_stopRequested) {
          return tableReader.next();
        }
        return null;
      } catch (NoSuchElementException e) {
        return null;
      } catch (SQLException e) {
        throw new RetriableException(e);
      }
    }, SQL_RETRY_PERIOD, _sqlTimeout);
  }

  private MysqlTableReader createTableReaderWithRetries() {
    MysqlSource source;
    try {
      source = MysqlSource.createFromUri(_datastreamTask.getDatastreamSource().getConnectionString());
    } catch (SourceNotValidException e) {
      String msg =
          String.format("SourceUri %s is invalid", _datastreamTask.getDatastreamSource().getConnectionString());
      LOG.error(msg, e);
      throw new DatastreamRuntimeException(msg, e);
    }

    return RetryUtils.retry(() -> {
      try {
        if (_tableInfoProvider == null) {
          _tableInfoProvider = new MysqlServerTableInfoProvider(source, _username, _password);
        }

        return new MysqlTableReader(source, _username, _password, _tableInfoProvider, _bufferSize);
      } catch (SQLException e) {
        throw new RetriableException(e);
      }
    }, SQL_RETRY_PERIOD, _sqlTimeout);
  }

  private void sendEvent(BrooklinEnvelope event) {
    // TODO producer error handling.
    _eventProducer.send(buildProducerRecord(event), null);
  }

  private DatastreamProducerRecord buildProducerRecord(BrooklinEnvelope event) {
    DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
    builder.addEvent(event);
    return builder.build();
  }

  public void stop() {
    if (_stopped) {
      return;
    }

    _stopRequested = true;
  }
}
