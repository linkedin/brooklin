package com.linkedin.datastream.testutil.eventGenerator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;


public class FixedLengthSchemaField extends SchemaField {

  int fixedSize;

  public FixedLengthSchemaField(Field field) {
    super(field);
    fixedSize = field.schema().getFixedSize();
  }

  @Override
  public void writeToRecord(GenericRecord record) throws UnknownTypeException {
    record.put(_field.name(), generateFixedLengthString());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException {
    return generateFixedLengthString();
  }

  public String generateFixedLengthString() {
    return _randGenerator.getNextString(0, fixedSize);
  }
}
