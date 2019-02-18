/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.sql.SQLException;

import org.apache.avro.Schema;


/**
 * Convert a ResultSet column read from the underlying database into avro compatible form.
 * Based on the underlying JDBC driver specifics, various marshalling might be required to interpret the data.
 */
public interface SqlTypeInterpreter {
  /**
   * Convert from underlying JDBC driver specific type to avro compatible type.
   * @param sqlObject Object read from the ResultSet.getObject() call
   * @param colName Name of the column read
   * @param avroSchema
   * @return
   * @throws SQLException
   */
  public Object sqlObjectToAvro(Object sqlObject, String colName, Schema avroSchema) throws SQLException;

  /**
   * Driver could change the column names causing incompatiblity with output avro. Reformat to correct
   * naming scheme. For example Column names are declared in UPPER_CAMEL in Oracle but avro field names are
   * LOWER_CAMEL.
   * @param dbColName
   * @return
   */
  public String formatColumnName(String dbColName);

}
