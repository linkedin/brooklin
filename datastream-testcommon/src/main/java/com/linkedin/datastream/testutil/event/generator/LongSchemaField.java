package com.linkedin.datastream.testutil.event.generator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;


public class LongSchemaField extends SchemaField {

  public LongSchemaField(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(IndexedRecord record) {
    record.put(_field.pos(), generateLong());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException {
    return generateLong();
  }

  public Long generateLong() {
    return _randGenerator.getNextLong();
  }
}
