/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil.event.generator;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;

import com.linkedin.datastream.testutil.common.RandomValueGenerator;


/**
 * The class is a factory which returns an instance based on which random data can be written to the record.
 */
public abstract class SchemaField {

  static RandomValueGenerator _randGenerator = null;
  static long _seed = -1;
  static int _minValue = 0;
  static int _maxValue = Integer.MAX_VALUE;
  static int _maxNumElements = 10;
  protected Field _field;

  /**
   * Constructor to create a schema field instance of the given field.
   * @param field The type of field to create a filler for.
   */
  public SchemaField(Field field) {
    _field = field;
    if (_randGenerator == null) { // true first time only if no external seed is given
      _seed = System.currentTimeMillis();
      createRandomValueGenerator();
    }
  }

  private static void createRandomValueGenerator() {
    _randGenerator = new RandomValueGenerator(_seed);
  }

  public static void setDataRange(int minValue, int maxValue) {
    _minValue = minValue;
    _maxValue = maxValue;
  }

  public static void setMaxNumElements(int maxNumElements) {
    _maxNumElements = maxNumElements;
  }

  public static long getSeed() {
    return _seed;
  }

  public static void setSeed(long seed) {
    _seed = seed;
    createRandomValueGenerator();
  }

  public static boolean isSeedSet() {
    return _seed >= 0;
  }

  /**
   * Factory method to generate random data according to type
   * @param field The field based on which random data is be generated
   * @return SchemaField The SchemaField instance let's you write data to the record based on the field passed to the function.
   */
  public static SchemaField createField(Field field) throws UnknownTypeException {

    // if randGenerator == null, create one here
    Schema.Type type = field.schema().getType();
    switch (type) {
      case ARRAY:
        return new ArraySchemaField(field);
      case BOOLEAN:
        return new BooleanSchemaField(field);
      case BYTES:
        return new BytesSchemaField(field);
      case DOUBLE:
        return new DoubleSchemaField(field);
      case ENUM:
        return new EnumSchemaField(field);
      case FIXED:
        return new FixedLengthSchemaField(field);
      case FLOAT:
        return new FloatSchemaField(field);
      case INT:
        return new IntegerSchemaField(field);
      case LONG:
        return new LongSchemaField(field);
      case MAP:
        return new MapSchemaField(field);
      case NULL:
        return new NullSchemaField(field);
      case RECORD:
        return new RecordSchemaField(field);
      case STRING:
        return new StringSchemaField(field);
      case UNION:
        return new UnionSchemaField(field);
      default:
        throw new UnknownTypeException();
    }
  }

  /**
   * Override to write data
   * @param  record  The IndexedRecord to which the data is to be written.
   */
  public abstract void writeToRecord(IndexedRecord record) throws UnknownTypeException;

  /**
   * Return the random generated object. Use this to fetch the object instead of writing to an record.
   */
  public abstract Object generateRandomObject() throws UnknownTypeException;
}
