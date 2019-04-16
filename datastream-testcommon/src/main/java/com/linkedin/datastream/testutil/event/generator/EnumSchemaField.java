/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil.event.generator;

import java.util.List;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;


/**
 * A generator of random enum values for a specific {@link Field}
 */
public class EnumSchemaField extends SchemaField {
  List<String> enumStrings;

  /**
   * Constructor for EnumSchemaField
   * @param field the schema field to generate values for
   */
  public EnumSchemaField(Field field) {
    super(field);
    enumStrings = field.schema().getEnumSymbols();
  }

  @Override
  public void writeToRecord(IndexedRecord record) throws UnknownTypeException {
    record.put(_field.pos(), generateEnum());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException {
    return generateEnum();
  }

  /**
   * Generate a random enum value from the enum symbols in the schema of the encapsulated field
   */
  public String generateEnum() {
    return enumStrings.get(_randGenerator.getNextInt(0, enumStrings.size()));
  }
}
