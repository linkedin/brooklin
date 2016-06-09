package com.linkedin.datastream.connectors.mysql.or;

/*
 * Copyright 2015 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
