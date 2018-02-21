package com.linkedin.datastream.common;

import java.sql.SQLException;

import org.apache.avro.Schema;


/**
 * Pass through interpreter that just returns the input object without mutating it
 */
public class PassThroughSqlTypeInterpreter implements SqlTypeInterpreter {
  @Override
  public Object sqlObjectToAvro(Object sqlObject, String colName, Schema avroSchema) throws SQLException {
    return sqlObject;
  }

  @Override
  public String formatColumn(String dbColName) {
    return dbColName;
  }

}
