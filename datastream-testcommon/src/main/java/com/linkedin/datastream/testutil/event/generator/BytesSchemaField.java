package com.linkedin.datastream.testutil.event.generator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;


public class BytesSchemaField extends SchemaField {

  public BytesSchemaField(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(IndexedRecord record) {
    record.put(_field.pos(), generateBytes());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException {
    return generateBytes();
  }

  public byte[] generateBytes() {
    return _randGenerator.getNextBytes(_maxNumElements);
  }
}
