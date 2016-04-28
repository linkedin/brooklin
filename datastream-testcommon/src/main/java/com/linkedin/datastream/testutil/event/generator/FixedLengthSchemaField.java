package com.linkedin.datastream.testutil.event.generator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;


public class FixedLengthSchemaField extends SchemaField {

  int fixedSize;

  public FixedLengthSchemaField(Field field) {
    super(field);
    fixedSize = field.schema().getFixedSize();
  }

  @Override
  public void writeToRecord(IndexedRecord record) throws UnknownTypeException {
    record.put(_field.pos(), generateFixedLengthString());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException {
    return generateFixedLengthString();
  }

  public String generateFixedLengthString() {
    return _randGenerator.getNextString(0, fixedSize);
  }
}
