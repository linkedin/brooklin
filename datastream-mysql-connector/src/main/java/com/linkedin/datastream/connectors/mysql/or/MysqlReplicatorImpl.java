package com.linkedin.datastream.connectors.mysql.or;

import com.google.code.or.OpenReplicator;
import com.google.code.or.binlog.BinlogEventParser;
import com.google.code.or.binlog.BinlogParserListener;
import com.google.code.or.binlog.BinlogRowEventFilter;
import com.google.code.or.binlog.impl.AbstractBinlogParser;
import com.google.code.or.binlog.impl.ReplicationBasedBinlogParser;
import com.google.code.or.binlog.impl.event.DeleteRowsEvent;
import com.google.code.or.binlog.impl.event.DeleteRowsEventV2;
import com.google.code.or.binlog.impl.event.UpdateRowsEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEventV2;
import com.google.code.or.binlog.impl.event.WriteRowsEvent;
import com.google.code.or.binlog.impl.event.WriteRowsEventV2;
import com.google.code.or.binlog.impl.parser.AbstractRowEventParser;

import com.linkedin.datastream.connectors.mysql.MysqlSource;


public class MysqlReplicatorImpl extends OpenReplicator implements MysqlReplicator {
  private BinlogRowEventFilter _rowEventFilter;
  private BinlogParserListener _parserListener;

  public MysqlReplicatorImpl(MysqlSource source, String userName, String password, int serverId, BinlogRowEventFilter rowEventFilter,
      BinlogParserListener parserListener) {
    _rowEventFilter = rowEventFilter;
    _parserListener = parserListener;
    setHost(source.getHostName());
    setPort(source.getPort());
    setUser(userName);
    setPassword(password);
    setServerId(serverId);
  }


  @Override
  protected ReplicationBasedBinlogParser getDefaultBinlogParser() throws Exception {
    ReplicationBasedBinlogParser parser = super.getDefaultBinlogParser();
    parser.addParserListener(_parserListener);
    setBinlogRowEventFilter(parser, _rowEventFilter);
    return parser;
  }


  public static void setBinlogRowEventFilter(AbstractBinlogParser binlogParser, BinlogRowEventFilter rowFilter) {

    // This sets a custom Binlog row event filter for the writeRowsEventParser, updateRowsEventParser
    // and DeleteRowsEventParser
    if (binlogParser instanceof AbstractBinlogParser) {
      setBinlogRowEventFilterForEventType(binlogParser, WriteRowsEvent.EVENT_TYPE, rowFilter);
      setBinlogRowEventFilterForEventType(binlogParser, WriteRowsEventV2.EVENT_TYPE, rowFilter);

      setBinlogRowEventFilterForEventType(binlogParser, UpdateRowsEvent.EVENT_TYPE, rowFilter);
      setBinlogRowEventFilterForEventType(binlogParser, UpdateRowsEventV2.EVENT_TYPE, rowFilter);

      setBinlogRowEventFilterForEventType(binlogParser, DeleteRowsEvent.EVENT_TYPE, rowFilter);
      setBinlogRowEventFilterForEventType(binlogParser, DeleteRowsEventV2.EVENT_TYPE, rowFilter);
    }
  }

  public static void setBinlogRowEventFilterForEventType(AbstractBinlogParser r, int eventType,
      BinlogRowEventFilter rowFilter) {
    BinlogEventParser eventParser = r.getEventParser(eventType);
    if (eventParser != null && eventParser instanceof AbstractRowEventParser) {
      AbstractRowEventParser rowEventParser = (AbstractRowEventParser) eventParser;
      rowEventParser.setRowEventFilter(rowFilter);
    }
  }
}

