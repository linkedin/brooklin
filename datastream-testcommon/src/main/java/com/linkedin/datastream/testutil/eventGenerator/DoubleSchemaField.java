package com.linkedin.datastream.testutil.eventGenerator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;


public class DoubleSchemaField extends SchemaField {

  public DoubleSchemaField(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(GenericRecord record) {
    record.put(_field.name(), generateDouble());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException {
    return generateDouble();
  }

  public Double generateDouble() {
    return _randGenerator.getNextDouble();
  }
}
