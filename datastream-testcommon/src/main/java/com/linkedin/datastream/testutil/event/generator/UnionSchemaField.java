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
 * A union generator for a specific {@link org.apache.avro.Schema.Field}
 */
public class UnionSchemaField extends SchemaField {

  /**
   * Construct an instance of UnionSchemaField using given {@link Field}
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
   * generate a union field
   */
  public SchemaField getUnionFieldField() throws UnknownTypeException {
    Optional<Schema> schema =
        _field.schema().getTypes().stream().filter(s -> s.getType() != Schema.Type.NULL).findFirst();

    return SchemaField.createField(new Field(_field.name(), schema.orElse(null), null, null));
  }
}
