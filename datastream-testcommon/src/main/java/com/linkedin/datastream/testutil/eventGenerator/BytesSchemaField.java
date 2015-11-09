package com.linkedin.datastream.testutil.eventGenerator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;


public class BytesSchemaField extends SchemaField {

  public BytesSchemaField(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(GenericRecord genericRecord) {
    genericRecord.put(_field.name(), generateBytes());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException {
    return generateBytes();
  }

  public byte[] generateBytes() {
    return _randGenerator.getNextBytes(_maxNumElements);
  }
}
