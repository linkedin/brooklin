package com.linkedin.datastream.connectors.mysql;

import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class TestMysqlCheckpoint {

  @Test
  public void testMysqlCheckpointBasics() {
    String sourceId = "serverid";
    long transaction = 1;
    int binlogFile = 10;
    long offsetInBinlog = 10000;
    String cpStr = MysqlCheckpoint.createCheckpointString(sourceId, transaction, binlogFile, offsetInBinlog);
    MysqlCheckpoint mysqlCheckpoint = new MysqlCheckpoint(cpStr);
    Assert.assertEquals(mysqlCheckpoint.getSourceIdStr(), sourceId);
    Assert.assertEquals(mysqlCheckpoint.getTransactionId(), transaction);
    Assert.assertEquals(mysqlCheckpoint.getBinlogFileNum(), binlogFile);
    Assert.assertEquals(mysqlCheckpoint.getBinlogOffset(), offsetInBinlog);
    Assert.assertEquals(mysqlCheckpoint.getCheckpointStr(), cpStr);
  }
}
