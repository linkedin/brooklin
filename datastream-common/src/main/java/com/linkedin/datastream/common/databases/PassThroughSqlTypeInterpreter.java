/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common.databases;

import java.sql.SQLException;

import org.apache.avro.Schema;

import com.linkedin.datastream.common.SqlTypeInterpreter;


/**
 * Pass through interpreter that just returns the input object without mutating it
 */
public class PassThroughSqlTypeInterpreter implements SqlTypeInterpreter {
  @Override
  public Object sqlObjectToAvro(Object sqlObject, String colName, Schema avroSchema) throws SQLException {
    return sqlObject;
  }

  @Override
  public String formatColumnName(String dbColName) {
    return dbColName;
  }

}
