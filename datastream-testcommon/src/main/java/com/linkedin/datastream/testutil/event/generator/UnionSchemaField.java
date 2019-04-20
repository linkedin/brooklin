/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil.event.generator;

import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;

/**
 * A generator of random union fields for a specific {@link Field}
 */
public class UnionSchemaField extends SchemaField {

  /**
   * Constructor for UnionSchemaField
   * @param field the schema field to generate values for
   */
  public UnionSchemaField(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(IndexedRecord record) throws UnknownTypeException {
    getUnionFieldField().writeToRecord(record);
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException {
    return getUnionFieldField().generateRandomObject();
  }

  /**
   * Generate a union field with the first schema that is not
   * {@code Schema.Type.NULL} in the schema of the encapsulated field
   */
  public SchemaField getUnionFieldField() throws UnknownTypeException {
    Optional<Schema> schema =
        _field.schema().getTypes().stream().filter(s -> s.getType() != Schema.Type.NULL).findFirst();

    return SchemaField.createField(new Field(_field.name(), schema.orElse(null), null, null));
  }
}
