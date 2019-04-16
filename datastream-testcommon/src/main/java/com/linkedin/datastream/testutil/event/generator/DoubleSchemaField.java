/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil.event.generator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;


/**
 * A generator of random double values for a specific {@link Field}
 */
public class DoubleSchemaField extends SchemaField {

  /**
   * Constructor for DoubleSchemaField
   * @param field the schema field to generate values for
   */
  public DoubleSchemaField(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(IndexedRecord record) {
    record.put(_field.pos(), generateDouble());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException {
    return generateDouble();
  }

  /**
   * Generates a random double value between {@code 0.0} and {@code 1.0} (inclusive)
   */
  public Double generateDouble() {
    return _randGenerator.getNextDouble();
  }
}
