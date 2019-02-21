package com.linkedin.datastream.testutil.event.generator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;


public class NullSchemaField extends SchemaField {

  public NullSchemaField(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(IndexedRecord record) {
    record.put(_field.pos(), null);
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException {
    return null;
  }
}
