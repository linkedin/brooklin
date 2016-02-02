package com.linkedin.datastream.testutil.event.generator;

import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;


public class UnionSchemaField extends SchemaField {

  public UnionSchemaField(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(GenericRecord record) throws UnknownTypeException {
    getUnionFieldField().writeToRecord(record);
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException {
    return getUnionFieldField().generateRandomObject();
  }

  public SchemaField getUnionFieldField() throws UnknownTypeException {
    Optional<Schema> schema =
        _field.schema().getTypes().stream().filter(s -> s.getType() != Schema.Type.NULL).findFirst();

    return SchemaField.createField(new Field(_field.name(), schema.orElse(null), null, null));
  }

}
