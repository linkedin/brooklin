package com.linkedin.datastream.avrogenerator;

/**
 * This public enum which allows us to map Oracle Database Primitive Types to Avro Primitive Types.
 * We classify all INTs, FLOATs, DOUBLEs, and LONGs as "string" under NUMBER. The metadata of will
 * contain Scale and Precision information
 */
public enum Types {
  NUMBER("string"),
  FLOAT("float"),
  INTEGER("int"),
  LONG("long"),
  DOUBLE("double"),
  RAW("bytes"),
  CLOB("string"),
  VARCHAR("string"),
  VARCHAR2("string"),
  NVARCHAR("string"),
  NVARCHAR2("string"),
  TIMESTAMP("long"),
  CHAR("string"),
  DATE("long"),
  BLOB("bytes"),
  ARRAY("array"),
  TABLE("record"),
  XMLTYPE("string");

  private final String _avroType;

  Types(String avroType) {
    _avroType = avroType;
  }

  public String getAvroType() {
    return _avroType;
  }

}
