package com.linkedin.datastream.avrogenerator;

import java.util.HashSet;


/**
 * light weight class to act as a wrapper around primitive Oracle Database Types
 * These includes types such as CHAR, VARCHAR2 etc.
 */
public class OraclePrimitiveType implements FieldType {
  private Types _type;
  private final int _scale;
  private final int _precision;

  private final static HashSet<String> NUMBER_CLASSIFICATION = new HashSet<>();
  static {
    NUMBER_CLASSIFICATION.add(Types.INTEGER.toString());
    NUMBER_CLASSIFICATION.add(Types.LONG.toString());
    NUMBER_CLASSIFICATION.add(Types.FLOAT.toString());
    NUMBER_CLASSIFICATION.add(Types.DOUBLE.toString());
    NUMBER_CLASSIFICATION.add(Types.NUMBER.toString());
  }

  public OraclePrimitiveType(String fieldTypeName, int scale, int precision) {
    _scale = scale;
    _precision = precision;

    if (fieldTypeName.equals(Types.NUMBER.toString())) {
      _type = getNumberClassification(scale, precision);
      return;
    }

    if (fieldTypeName.startsWith("SYS.")) {
      // to remove the SYS. prefix for XMLTYPE;
      fieldTypeName = fieldTypeName.substring(4);
    }

    _type = Types.fromString(fieldTypeName);
  }

  @Override
  public String getSchemaName() {
    return  null;
  }

  @Override
  public String getFieldTypeName() {
    return _type.toString();
  }

  @Override
  public String getAvroFieldName() {
    return _type.getAvroType();
  }

  @Override
  public String toString() {
    return String.format("PrimitiveType: %s, AvroType: %s", getFieldTypeName(), getAvroFieldName());
  }

  @Override
  public AvroJson toAvro() {
    AvroJson primitiveRecord = new AvroJson();

    // Field type
    String[] type = new String[] {"null", getAvroFieldName()};
    primitiveRecord.setType(type);

    return primitiveRecord;
  }

  public int getPrecision() {
    return _precision;
  }

  public int getScale() {
    return _scale;
  }

  /**
   * If this OraclePrimitiveType instance is storing a Number field, we want to pass in
   * the Scale and Precision in the metadata.
   */
  @Override
  public String getMetadata() {
    StringBuilder meta = new StringBuilder();
    meta.append(String.format("%s=%s;", FIELD_TYPE_NAME, getFieldTypeName()));

    if (NUMBER_CLASSIFICATION.contains(getFieldTypeName())) {
      meta.append(String.format("%s=%s;", SCALE, getScale()));
      meta.append(String.format("%s=%s;", PRECISION, getPrecision()));
    }

    return meta.toString();
  }


  /**
   * Numeric Type can be better specified by Avro Primitives based on their scale and precision
   * The following function will classify the type store in Oracle based on the following:
   *
   * If 0 < scale <= 6  ===> FLOAT (Irrespective of the precision)
   * If 7 <= scale <= 17 ===> DOUBLE (Irrespective of the precision)
   * If precision > 9 and scale <= 0 ===> LONG
   * If precision <= 9 and scale <= 0 ===> INTEGER
   *
   * If we dont have enough information to accurately clasify a number, we use
   * Types.NUMBER which defaults to string in avro
   *
   * If precision == 0 and scale <= 0 ====> NUMBER -> string
   *
   * @param precision the number of significant digits stored as the NUMERIC in Oracle
   * @param scale The number of digits to the (right) or left (negative) of the decimal point
   */
  private static Types getNumberClassification(int scale, int precision) {
    if (scale > 0) {
      if (scale <= 6) {
        return Types.FLOAT;
      } else if (scale <= 17) {
        return Types.DOUBLE;
      } else {
        throw new RuntimeException("Cannot handle scale of greater than 17");
      }
    }

    if (precision > 9) {
      return Types.LONG;
    }

    if (precision == 0) {
      return Types.NUMBER;
    }

    return Types.INTEGER;
  }
}
