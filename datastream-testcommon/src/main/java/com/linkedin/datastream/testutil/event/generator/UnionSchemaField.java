package com.linkedin.datastream.testutil.event.generator;

import java.util.List;
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
    List<Schema> schemas = _field.schema().getTypes();
    Schema schema = null;
    for (Schema s : schemas) {
      schema = s;
      if (schema.getType() != Schema.Type.NULL)
        break;
    }
    Field tempField = new Field(_field.name(), schema, null, null);
    return SchemaField.createField(tempField);
  }

}
