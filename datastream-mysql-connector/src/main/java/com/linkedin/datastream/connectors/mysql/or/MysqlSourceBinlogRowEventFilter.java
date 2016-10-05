package com.linkedin.datastream.connectors.mysql.or;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.binlog.BinlogParserContext;
import com.google.code.or.binlog.BinlogRowEventFilter;
import com.google.code.or.binlog.impl.event.TableMapEvent;

import com.linkedin.datastream.metrics.BrooklinMetric;
import com.linkedin.datastream.metrics.DynamicBrooklinMetric;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.metrics.MetricsAware;


public class MysqlSourceBinlogRowEventFilter implements BinlogRowEventFilter {
  private static final String TOTAL_EVENTS_RATE = "totalEvents";
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
    _dynamicMetricsManager.createOrUpdateMeter(this.getClass(), _taskName, TOTAL_EVENTS_RATE, 1);
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

  public static List<BrooklinMetric> getMetrics() {
    List<BrooklinMetric> metrics = new ArrayList<>();
    metrics.add(new DynamicBrooklinMetric(CLASSNAME + MetricsAware.KEY_REGEX + TOTAL_EVENTS_RATE,
        BrooklinMetric.MetricType.METER));
    return Collections.unmodifiableList(metrics);
  }
}
