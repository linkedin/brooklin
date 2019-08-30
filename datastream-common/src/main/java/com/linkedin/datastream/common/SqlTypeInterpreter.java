/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.sql.SQLException;

import org.apache.avro.Schema;


/**
 * Convert a ResultSet column read from the underlying database into Avro compatible form.
 * Based on the underlying JDBC driver specifics, various marshalling might be required to interpret the data.
 */
public interface SqlTypeInterpreter {
  /**
   * Convert from underlying JDBC driver specific type to Avro compatible type.
   * @param sqlObject Object read from the ResultSet.getObject() call
   * @param colName Name of the column read
   * @param avroSchema the Avro schema to use
   */
  Object sqlObjectToAvro(Object sqlObject, String colName, Schema avroSchema) throws SQLException;

  /**
   * Driver could change the column names causing incompatibility with output Avro. Reformat to correct
   * naming scheme. For example Column names are declared in UPPER_CAMEL in Oracle but Avro field names are
   * LOWER_CAMEL.
   */
  String formatColumnName(String dbColName);
}
