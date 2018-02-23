package com.linkedin.datastream.connectors.oracle.triggerbased.consumer;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.List;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.datastream.common.databases.DatabaseColumnRecord;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.connectors.oracle.triggerbased.MockSchema;


@Test
public class TestOracleTbTableReader {
  @Test
  public void testGenerateEvent() throws SQLException {
    ResultSet rs = Mockito.mock(ResultSet.class);
    ResultSetMetaData rsmd = Mockito.mock(ResultSetMetaData.class);

    Mockito.when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    Mockito.when(rs.getMetaData()).thenReturn(rsmd);

    Mockito.when(rsmd.getColumnCount()).thenReturn(4);

    Mockito.when(rsmd.getColumnType(3)).thenReturn(Types.NUMERIC);
    Mockito.when(rsmd.getColumnType(4)).thenReturn(Types.NUMERIC);
    Mockito.when(rsmd.getColumnName(3)).thenReturn("TXN");
    Mockito.when(rsmd.getColumnName(4)).thenReturn("GET_LUCKY");

    Mockito.when(rs.getObject(3)).thenReturn(BigDecimal.valueOf(1));
    Mockito.when(rs.getObject(4)).thenReturn(BigDecimal.valueOf(10L));

    // do not throw error
    OracleChangeEvent event =
        OracleTbTableReader.generateEvent(rs, MockSchema.GENERIC_SCHEMA, 10L, 100L);

    Assert.assertEquals(event.getScn(), 10L);
    Assert.assertEquals(event.getSourceTimestamp(), 100L);

    List<DatabaseColumnRecord> records = event.getRecords();
    DatabaseColumnRecord txnRecord = records.get(0);
    DatabaseColumnRecord keyRecord = records.get(1);

    Assert.assertEquals(txnRecord.getColName(), "txn");
    Assert.assertEquals(txnRecord.getValue(), 1L);
    Assert.assertEquals(txnRecord.getSqlType(), Types.NUMERIC);
    Assert.assertEquals(keyRecord.getColName(), "getLucky");
    Assert.assertEquals(keyRecord.getValue(), 10L);
    Assert.assertEquals(keyRecord.getSqlType(), Types.NUMERIC);
  }

  @Test(expectedExceptions = DatastreamRuntimeException.class)
  public void testGenerateEventInvalidType() throws SQLException {
    ResultSet rs = Mockito.mock(ResultSet.class);
    ResultSetMetaData rsmd = Mockito.mock(ResultSetMetaData.class);

    Mockito.when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    Mockito.when(rs.getMetaData()).thenReturn(rsmd);

    Mockito.when(rsmd.getColumnCount()).thenReturn(4);

    Mockito.when(rsmd.getColumnType(3)).thenReturn(Types.NUMERIC);
    Mockito.when(rsmd.getColumnType(4)).thenReturn(Types.NUMERIC);
    Mockito.when(rsmd.getColumnName(3)).thenReturn("TXN");
    Mockito.when(rsmd.getColumnName(4)).thenReturn("GET_LUCKY");

    // Instant instances should not be converted
    Mockito.when(rs.getObject(3)).thenReturn(Instant.now());
    Mockito.when(rs.getObject(4)).thenReturn(BigDecimal.valueOf(10L));

    OracleChangeEvent event =
        OracleTbTableReader.generateEvent(rs, MockSchema.GENERIC_SCHEMA, 10L, 100L);
  }

  @Test(expectedExceptions = DatastreamRuntimeException.class)
  public void testGenerateEventInvalidCount() throws SQLException {
    ResultSet rs = Mockito.mock(ResultSet.class);
    ResultSetMetaData rsmd = Mockito.mock(ResultSetMetaData.class);

    Mockito.when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    Mockito.when(rs.getMetaData()).thenReturn(rsmd);

    Mockito.when(rsmd.getColumnCount()).thenReturn(2);

    OracleChangeEvent event =
        OracleTbTableReader.generateEvent(rs, MockSchema.GENERIC_SCHEMA, 10L, 100L);
  }
}
