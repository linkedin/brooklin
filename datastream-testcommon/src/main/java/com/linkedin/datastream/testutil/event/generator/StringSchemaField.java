/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil.event.generator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;


/**
 * A random string generator for a specific {@link org.apache.avro.Schema.Field}
 */
public class StringSchemaField extends SchemaField {

  /**
   * Construct an instance of StringSchemaField using given {@link Field}
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
   * generate a random string
   */
  public String generateString() {
    return _randGenerator.getNextString(1, _maxNumElements);
  }
}
