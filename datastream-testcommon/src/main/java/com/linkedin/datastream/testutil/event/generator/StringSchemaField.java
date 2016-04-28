package com.linkedin.datastream.testutil.event.generator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;


public class StringSchemaField extends SchemaField {

  public StringSchemaField(Field field) {
    super(field);
    // todo - do we want a default mode that changes the default sizes for each of these fields ??
  }

  @Override
  public void writeToRecord(IndexedRecord record) {
    record.put(_field.pos(), generateString());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException {
    return generateString();
  }

  public String generateString() {
    return _randGenerator.getNextString(1, _maxNumElements);
  }

}
