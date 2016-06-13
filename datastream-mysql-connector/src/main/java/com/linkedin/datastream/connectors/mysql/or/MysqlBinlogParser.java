package com.linkedin.datastream.connectors.mysql.or;

import com.google.code.or.OpenParser;
import com.google.code.or.binlog.BinlogParserListener;
import com.google.code.or.binlog.BinlogRowEventFilter;
import com.google.code.or.binlog.impl.FileBasedBinlogParser;


/**
 * Wrapper around the OpenParser.
 * OpenParser allows us to parse the binlog files directly and generate events.
 */
public class MysqlBinlogParser extends OpenParser implements MysqlReplicator {

  private final BinlogParserListener _parserListener;
  private final BinlogRowEventFilter _rowEventFilter;
  public static final String BINLOG_FILENAME_FORMAT = "mysql-bin.%06d";

  public MysqlBinlogParser(String binlogFolder, BinlogRowEventFilter rowEventFilter,
      BinlogParserListener parserListener) {
    setBinlogFilePath(binlogFolder);
    setBinlogFileName(String.format(BINLOG_FILENAME_FORMAT, 1));
    setStartPosition(0);
    _rowEventFilter = rowEventFilter;
    _parserListener = parserListener;
  }

  @Override
  protected FileBasedBinlogParser getDefaultBinlogParser() throws Exception {
    FileBasedBinlogParser parser = super.getDefaultBinlogParser();
    parser.addParserListener(_parserListener);
    setBinlogRowEventFilter(parser, _rowEventFilter);
    return parser;
  }

  public void setBinlogRowEventFilter(FileBasedBinlogParser parser, BinlogRowEventFilter rowFilter) {
    MysqlReplicatorImpl.setBinlogRowEventFilter(parser, rowFilter);
  }
}
