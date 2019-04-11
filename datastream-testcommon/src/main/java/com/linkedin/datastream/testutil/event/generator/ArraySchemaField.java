/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil.event.generator;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;

/**
 * Generate a record whose value is array
 */
public class ArraySchemaField extends SchemaField {

  private static int maxArrayLength = 10;

  /**
   * Construct an instance of ArraySchemaField using given {@link Field}
   */
  public ArraySchemaField(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(IndexedRecord record) throws UnknownTypeException {
    record.put(_field.pos(), generateArray());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException {
    return generateArray();
  }

  /**
   * Generate an array by the filed
   * @throws UnknownTypeException the unknown type exception
   */
  public GenericData.Array<Object> generateArray() throws UnknownTypeException {
    Schema innerElementSchema = _field.schema().getElementType();
    int numElements = _randGenerator.getNextInt(1, _maxNumElements);
    GenericData.Array<Object> array = new GenericData.Array<Object>(numElements, _field.schema());

    for (int i = 0; i < numElements; i++) {
      Field fakeField = new Field(_field.name() + "fake", innerElementSchema, null, null);
      SchemaField schemaFill = SchemaField.createField(fakeField); //Safe from infinite recursion (array within an array, assuming nullable)
      array.add(schemaFill.generateRandomObject());
    }
    return array;
  }
}
