/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil.event.generator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;

/**
 * A generator of random Long values for a specific {@link Field}
 */
public class LongSchemaField extends SchemaField {

  /**
   * Constructor for LongSchemaField
   * @param field the schema field to generate values for
   */
  public LongSchemaField(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(IndexedRecord record) {
    record.put(_field.pos(), generateLong());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException {
    return generateLong();
  }

  /**
   * Generate a random Long value between {@code 0} and {@code Long.MAX_VALUE}, inclusive
   */
  public Long generateLong() {
    return _randGenerator.getNextLong();
  }
}
