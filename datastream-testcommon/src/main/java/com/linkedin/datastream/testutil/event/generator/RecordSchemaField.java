/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil.event.generator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

/**
 * A generator of random {@link GenericRecord} objects for a specific {@link Field}
 */
public class RecordSchemaField extends SchemaField {

  /**
   * Constructor for RecordSchemaField
   * @param field the schema field to generate values for
   */
  public RecordSchemaField(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(IndexedRecord record) throws UnknownTypeException {

    record.put(_field.pos(), generateRecord());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException {
    return generateRecord();
  }

  /**
   * Generate a random GenericRecord with all fields set to random values
   */
  public GenericRecord generateRecord() throws UnknownTypeException {
    GenericRecord subRecord = new GenericData.Record(_field.schema());
    for (Field field : _field.schema().getFields()) {
      SchemaField fill = SchemaField.createField(field);
      fill.writeToRecord(subRecord);
    }

    return subRecord;
  }
}
