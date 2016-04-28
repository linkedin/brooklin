package com.linkedin.datastream.testutil.event.generator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;


public class IntegerSchemaField extends SchemaField {

  public IntegerSchemaField(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(IndexedRecord record) {
    record.put(_field.pos(), generateInteger());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException {
    return generateInteger();
  }

  public Integer generateInteger() {
    return _randGenerator.getNextInt(_minValue, _maxValue);
  }

}
