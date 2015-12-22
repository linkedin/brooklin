package com.linkedin.datastream.testutil.event.generator;

import java.util.List;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;


public class EnumSchemaField extends SchemaField {
  List<String> enumStrings;

  public EnumSchemaField(Field field) {
    super(field);
    enumStrings = field.schema().getEnumSymbols();

  }

  @Override
  public void writeToRecord(GenericRecord record) throws UnknownTypeException {
    record.put(_field.name(), generateEnum());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException {
    return generateEnum();
  }

  public String generateEnum() {
    return enumStrings.get(_randGenerator.getNextInt(0, enumStrings.size()));
  }

}
