package com.linkedin.datastream.testutil.event.generator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;


public class BooleanSchemaField extends SchemaField {

  public BooleanSchemaField(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(GenericRecord record) throws UnknownTypeException {
    record.put(_field.name(), generateBoolean());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException {
    return generateBoolean();
  }

  public boolean generateBoolean() {
    return _randGenerator.getNextBoolean();
  }
}
