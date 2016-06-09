package com.linkedin.datastream.connectors.mysql.or;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.or.binlog.BinlogParser;
import com.google.code.or.binlog.BinlogParserListener;


/**
 * We are interested in the state of the parser
 * This class will allow us to react to the different events
 * that happen during parsing.
 */
public class MysqlBinlogParserListener extends BinlogParserListener.Adapter {
  private static final Logger LOG = LoggerFactory.getLogger(MysqlBinlogParserListener.class);

  @Override
  public void onStart(BinlogParser parser) {
    LOG.info("Started producer");
  }

  @Override
  public void onStop(BinlogParser parser) {
    LOG.info("Stopped producer");
  }

  @Override
  public void onException(BinlogParser parser, Exception exception) {
    LOG.warn("Exception was caught in the binlog parser " + exception.toString());
  }
}
