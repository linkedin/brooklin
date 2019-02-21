/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil.event.generator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;


public class LongSchemaField extends SchemaField {

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

  public Long generateLong() {
    return _randGenerator.getNextLong();
  }
}
