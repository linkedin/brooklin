package com.linkedin.datastream.testutil.event.generator;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;


public class ArraySchemaField extends SchemaField {

  private static int maxArrayLength = 10;

  public ArraySchemaField(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(GenericRecord record) throws UnknownTypeException {
    record.put(_field.name(), generateArray());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException {
    return generateArray();
  }

  public GenericData.Array<Object> generateArray() throws UnknownTypeException {
    Schema innerElementSchema = _field.schema().getElementType();
    int numElements = _randGenerator.getNextInt(1, _maxNumElements);
    GenericData.Array<Object> array = new GenericData.Array<Object>(numElements, _field.schema());

    for (int i = 0; i < numElements; i++) {
      /* --> uglier way to fetch the random object
      Field fakeField = new Field(field.name()+"fake", innerElementSchema, null, null);
      List<Schema.Field> tlist = new ArrayList<Schema.Field>();
      tlist.add(fakeField);
      Schema fakeSchema = Schema.createRecord(tlist);
      GenericRecord tempRecord  = new GenericData.Record(fakeSchema);
      SchemaField schemaFill = SchemaField.createRandomField(fakeField);
      */
      Field fakeField = new Field(_field.name() + "fake", innerElementSchema, null, null);
      SchemaField schemaFill = SchemaField.createField(fakeField); //Safe from infinite recursion (array within an array, assuming nullable)
      array.add(schemaFill.generateRandomObject());
    }
    return array;
  }
}
