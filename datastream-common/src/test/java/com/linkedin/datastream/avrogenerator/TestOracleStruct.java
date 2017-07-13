package com.linkedin.datastream.avrogenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class TestOracleStruct {

  @Test
  public void testConstructorBasic() {
    OraclePrimitiveType primitive = new OraclePrimitiveType("VARCHAR2", 0, 0);
    OracleColumn col1 = new OracleColumn("colName1", primitive, 1);
    OracleColumn col2 = new OracleColumn("colName2", primitive, 2);

    List<OracleColumn> list = new ArrayList<>();
    list.add(col1);
    list.add(col2);

    OracleStructType struct = new OracleStructType("JOBS", "fieldName", list);

    Assert.assertNotNull(struct);
    Assert.assertEquals(struct.getAvroFieldName(), "record");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testToAvro() throws Exception {
    OraclePrimitiveType primitive = new OraclePrimitiveType("VARCHAR2", 0, 0);
    OracleColumn col1 = new OracleColumn("colName1", primitive, 1);
    OracleColumn col2 = new OracleColumn("colName2", primitive, 2);

    List<OracleColumn> list = new ArrayList<>();
    list.add(col1);
    list.add(col2);

    OracleStructType struct = new OracleStructType("JOBS", "fieldName", list);

    Map<String, Object> info = struct.toAvro().info();
    List<Object> types = (List<Object>) info.get("type");
    Map<String, Object> recordType = (Map<String, Object>) types.get(0);


    Assert.assertEquals((String) types.get(1), "null");
    Assert.assertEquals((String) recordType.get("type"), "record");
    Assert.assertEquals((String) recordType.get("name"), "fieldName");
    Assert.assertNotNull(recordType.get("fields"));
  }
}
