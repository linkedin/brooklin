package com.linkedin.datastream.connectors.mysql;

import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class TestMysqlSource {
  @Test
  public void testBasic() throws Exception {
    String hostName = "localhost";
    int port = 12345;
    String db = "db1";
    String table = "table1";
    MysqlSource source = new MysqlSource(hostName, port, db, table);
    Assert.assertEquals(source.getHostName(), hostName);
    Assert.assertEquals(source.getPort(), port);
    Assert.assertEquals(source.getDatabaseName(), db);
    Assert.assertEquals(source.getTableName(), table);
    MysqlSource source2 = MysqlSource.createFromUri(source.toString());
    Assert.assertEquals(source.getHostName(), source2.getHostName());
    Assert.assertEquals(source.getPort(), source2.getPort());
    Assert.assertEquals(source.getDatabaseName(), source2.getDatabaseName());
    Assert.assertEquals(source.getTableName(), source2.getTableName());
  }

}
