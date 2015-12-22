package com.linkedin.datastream.testutil.event.generator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;


public class FloatSchemaField extends SchemaField {

  public FloatSchemaField(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(GenericRecord record) {
    record.put(_field.name(), generateFloat());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException {
    return generateFloat();
  }

  public Float generateFloat() {
    return _randGenerator.getNextFloat();
  }
}
