package com.linkedin.datastream.connectors.mysql;

import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class TestMysqlCheckpoint {

  @Test
  public void testMysqlCheckpointBasics() {
    String sourceId = "serverid";
    long transaction = 1;
    String binlogFile = "mysql-bin.0001";
    long offsetInBinlog = 10000;
    String cpStr = MysqlCheckpoint.createCheckpointString(sourceId, transaction, binlogFile, offsetInBinlog);
    MysqlCheckpoint mysqlCheckpoint = new MysqlCheckpoint(cpStr);
    Assert.assertEquals(mysqlCheckpoint.getSourceIdStr(), sourceId);
    Assert.assertEquals(mysqlCheckpoint.getTransactionId(), transaction);
    Assert.assertEquals(mysqlCheckpoint.getBinlogFileName(), binlogFile);
    Assert.assertEquals(mysqlCheckpoint.getBinlogOffset(), offsetInBinlog);
    Assert.assertEquals(mysqlCheckpoint.getCheckpointStr(), cpStr);
  }
}
