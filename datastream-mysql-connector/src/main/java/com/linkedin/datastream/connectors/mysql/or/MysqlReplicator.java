package com.linkedin.datastream.connectors.mysql.or;

import java.util.concurrent.TimeUnit;


public interface MysqlReplicator {
  void start() throws Exception;

  void stop(long timeout, TimeUnit timeUnit)  throws Exception;

  boolean isRunning();
}
