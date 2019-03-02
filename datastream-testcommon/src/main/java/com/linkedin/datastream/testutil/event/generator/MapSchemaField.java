/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil.event.generator;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;


public class MapSchemaField extends SchemaField {

  public MapSchemaField(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(IndexedRecord record) throws UnknownTypeException {
    record.put(_field.pos(), generateMap());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException {
    return generateMap();
  }

  public Map<Utf8, Object> generateMap() throws UnknownTypeException {
    int count = _randGenerator.getNextInt(1, _maxNumElements);
    Map<Utf8, Object> map = new HashMap<Utf8, Object>(count);
    Field fakeField = new Field(_field.name(), _field.schema().getValueType(), null, null);

    for (int i = 0; i < count; i++) {
      SchemaField filler = SchemaField.createField(fakeField); // create a new filler each time to emulate null-able fields
      map.put(new Utf8(_field.name() + i), filler.generateRandomObject());
    }
    return map;
  }
}
