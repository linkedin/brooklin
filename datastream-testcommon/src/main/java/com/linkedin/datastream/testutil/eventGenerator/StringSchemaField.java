package com.linkedin.datastream.testutil.eventGenerator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;


public class StringSchemaField extends SchemaField {

  public StringSchemaField(Field field) {
    super(field);
    // todo - do we want a default mode that changes the default sizes for each of these fields ??
  }

  @Override
  public void writeToRecord(GenericRecord genericRecord) {
    genericRecord.put(_field.name(), generateString());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException {
    return generateString();
  }

  public String generateString() {
    return _randGenerator.getNextString(1, _maxNumElements);
  }

}
