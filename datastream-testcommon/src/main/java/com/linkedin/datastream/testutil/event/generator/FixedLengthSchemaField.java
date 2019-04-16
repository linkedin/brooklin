/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil.event.generator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;

/**
 * A random fixed length string generator for a specific {@link org.apache.avro.Schema.Field}
 */
public class FixedLengthSchemaField extends SchemaField {

  int fixedSize;

  /**
   * Construct an instance of FixedLengthSchemaField using given {@link Field}
   */
  public FixedLengthSchemaField(Field field) {
    super(field);
    fixedSize = field.schema().getFixedSize();
  }

  @Override
  public void writeToRecord(IndexedRecord record) throws UnknownTypeException {
    record.put(_field.pos(), generateFixedLengthString());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException {
    return generateFixedLengthString();
  }

  /**
   * generate a random fixed length string
   */
  public String generateFixedLengthString() {
    return _randGenerator.getNextString(0, fixedSize);
  }
}
