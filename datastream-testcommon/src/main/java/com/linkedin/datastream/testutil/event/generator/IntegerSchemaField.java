/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil.event.generator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;

/**
 * A generator of random Integer values for a specific {@link Field}
 */
public class IntegerSchemaField extends SchemaField {

  /**
   * Constructor for IntegerSchemaField
   * @param field the schema field to generate values for
   */
  public IntegerSchemaField(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(IndexedRecord record) {
    record.put(_field.pos(), generateInteger());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException {
    return generateInteger();
  }

  /**
   * Generate a random Integer value between {@value _minValue} and {@value _maxValue}, inclusive
   */
  public Integer generateInteger() {
    return _randGenerator.getNextInt(_minValue, _maxValue);
  }
}
