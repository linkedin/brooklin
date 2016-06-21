package com.linkedin.datastream.connectors.mysql.or;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.binlog.BinlogParserContext;
import com.google.code.or.binlog.BinlogRowEventFilter;
import com.google.code.or.binlog.impl.event.TableMapEvent;

import com.linkedin.datastream.common.DynamicMetricsManager;
import com.linkedin.datastream.common.MetricsAware;


public class MysqlSourceBinlogRowEventFilter implements BinlogRowEventFilter {
  private static final String TOTAL_EVENTS_COUNT = "totalEvents";
  private static final String CLASSNAME = MysqlSourceBinlogRowEventFilter.class.getSimpleName();
  private final String _databaseName;
  private final String _tableName;
  private final Boolean _acceptAllTables;
  private final DynamicMetricsManager _dynamicMetricsManager;
  private final String _taskName;

  public MysqlSourceBinlogRowEventFilter(String taskName, String databaseName, Boolean acceptAllTables, String tableName,
      DynamicMetricsManager dynamicMetricsManager) {
    _taskName = taskName;
    _databaseName = databaseName;
    _tableName = tableName;
    _acceptAllTables = acceptAllTables;
    _dynamicMetricsManager = dynamicMetricsManager;
  }

  @Override
  public boolean accepts(BinlogEventV4Header header, BinlogParserContext context, TableMapEvent event) {
    _dynamicMetricsManager.createOrUpdateCounter(this.getClass(), _taskName, TOTAL_EVENTS_COUNT, 1);
    if (event.getDatabaseName() == null || event.getTableName() == null) {
      return false;
    }

    if (_databaseName == null) {
      return true;
    }

    if ((event.getDatabaseName().toString()).equalsIgnoreCase(_databaseName)) {
      if (_acceptAllTables) {
        return true;
      } else {
        return (event.getTableName().toString()).equalsIgnoreCase(_tableName);
      }
    } else {
      return false;
    }
  }

  public static Map<String, Metric> getMetrics() {
    Map<String, Metric> metrics = new HashMap<>();
    metrics.put(CLASSNAME + MetricsAware.KEY_REGEX  + TOTAL_EVENTS_COUNT, new Counter());
    return Collections.unmodifiableMap(metrics);
  }
}
