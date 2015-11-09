package com.linkedin.datastream.testutil.eventGenerator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;


public class NullSchemaField extends SchemaField {

  public NullSchemaField(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(GenericRecord record) {
    record.put(_field.name(), null);
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException {
    return null;
  }

}
