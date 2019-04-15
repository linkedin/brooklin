/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil.event.generator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;


/**
 * A random integer generator for a specific {@link org.apache.avro.Schema.Field}
 */
public class IntegerSchemaField extends SchemaField {

  /**
   * Construct an instance of IntegerSchemaField using given {@link Field}
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
   * generate a random integer
   */
  public Integer generateInteger() {
    return _randGenerator.getNextInt(_minValue, _maxValue);
  }
}
