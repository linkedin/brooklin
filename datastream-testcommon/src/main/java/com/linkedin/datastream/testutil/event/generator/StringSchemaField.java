/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil.event.generator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;

/**
 * A generator of random strings for a specific {@link Field}
 */
public class StringSchemaField extends SchemaField {

  /**
   * Constructor for StringSchemaField
   * @param field the schema field to generate values for
   */
  public StringSchemaField(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(IndexedRecord record) {
    record.put(_field.pos(), generateString());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException {
    return generateString();
  }

  /**
   * Generate a random string of length between {@code 1} and {@value SchemaField#_maxNumElements}, inclusive
   */
  public String generateString() {
    return _randGenerator.getNextString(1, _maxNumElements);
  }
}
