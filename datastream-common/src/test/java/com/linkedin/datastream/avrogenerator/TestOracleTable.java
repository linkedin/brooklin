package com.linkedin.datastream.avrogenerator;

import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class TestOracleTable {

  @Test
  public void testConstructorBasic() {
    OraclePrimitiveType primitive = new OraclePrimitiveType("VARCHAR2", 0, 0);
    OracleColumn col1 = new OracleColumn("colName1", primitive, 1);
    OracleColumn col2 = new OracleColumn("colName2", primitive, 2);

    List<OracleColumn> list = new ArrayList<>();
    list.add(col1);
    list.add(col2);

    // should nto throw error
    OracleTable table = new OracleTable("tableName", "schemaName", list, "primaryKey");

    Assert.assertNotNull(table);
  }

  @Test
  public void testToAvro() throws Exception {
    OraclePrimitiveType primitive = new OraclePrimitiveType("VARCHAR2", 0, 0);
    OracleColumn col1 = new OracleColumn("colName1", primitive, 1);
    OracleColumn col2 = new OracleColumn("colName2", primitive, 2);

    List<OracleColumn> list = new ArrayList<>();
    list.add(col1);
    list.add(col2);

    OracleTable table = new OracleTable("tableName", "schemaName", list, "primaryKey");

    Schema fullSchema = table.toAvro().toSchema();

    Assert.assertEquals(fullSchema.getNamespace(), "com.linkedin.events.schemaname");
    Assert.assertEquals(fullSchema.getProp("meta"), "dbTableName=tableName;pk=primaryKey;");
    Assert.assertTrue(fullSchema.getType().equals(Schema.Type.RECORD));
  }
}
