package com.linkedin.datastream.testutil.event.generator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;


public class FloatSchemaField extends SchemaField {

  public FloatSchemaField(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(IndexedRecord record) {
    record.put(_field.pos(), generateFloat());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException {
    return generateFloat();
  }

  public Float generateFloat() {
    return _randGenerator.getNextFloat();
  }
}
