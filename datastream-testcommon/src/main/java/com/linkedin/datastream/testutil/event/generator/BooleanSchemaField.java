/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil.event.generator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;


public class BooleanSchemaField extends SchemaField {

  public BooleanSchemaField(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(IndexedRecord record) throws UnknownTypeException {
    record.put(_field.pos(), generateBoolean());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException {
    return generateBoolean();
  }

  public boolean generateBoolean() {
    return _randGenerator.getNextBoolean();
  }
}
