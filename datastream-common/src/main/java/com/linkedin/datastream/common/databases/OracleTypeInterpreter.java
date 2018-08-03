package com.linkedin.datastream.common.databases;

import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;

import com.linkedin.datastream.avrogenerator.FieldMetadata;
import com.linkedin.datastream.avrogenerator.Types;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.SqlTypeInterpreter;


/**
 * Util class to convert Oracle JDBC ResultSet fields into a avro compatible format
 */
public class OracleTypeInterpreter implements SqlTypeInterpreter {
  private static final Logger LOG = LoggerFactory.getLogger(OracleTypeInterpreter.class);

  private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
  private static final String META_KEY = "meta";

  private final static List<Schema.Type> ACCEPTABLE_PRIMITIVES =
      Arrays.asList(Schema.Type.INT, Schema.Type.FLOAT, Schema.Type.LONG, Schema.Type.DOUBLE, Schema.Type.STRING);
  private final static List<Schema.Type> ACCEPTABLE_RECORD = Arrays.asList(Schema.Type.RECORD);
  private final static List<Schema.Type> ACCEPTABLE_COLLECTION = Arrays.asList(Schema.Type.ARRAY);
  private static final Map<String, String> COLUMN_NAME_CACHE = new ConcurrentHashMap<>();


  /**
   * Fields are built in order to have better compatibility with Avro Schemas rather
   * than the original ResultSet. In other words sql CHARS and VARCHARS are converted to Strings
   * and, NUMERIC and TIMESTAMP are converted to Long
   *
   * We iterate through the ResultSet each time calling {@code rs.getObject(index)}.
   * The {@code #getObject(int index)} will return an Java Object that follows a predefined
   * mapping. For example, a CHAR stored in the ResultSet will be returned as an
   * java.lang.String. In the base cases of this parser method, we simply need to extract the
   * correct value from these sqlObjects
   *
   * However, we need the type to be properly mapped to the expected type for that field in the
   * supplied avro Schema. For example a field {@code maxSalary} might be stored as Numeric in the
   * Oracle DB, and the avro Schema would expect an {@code int}. It's also possible for the avro Schema
   * to expect an {@code long} or {@code float}. Therefore, this parser needs to follow along with the
   * avro Schema, making sure we retrieve the correct types in the correct situations.
   *
   * The recursive parts come when we have to parse Structs and Arrays. both of these type have sub
   * elements, and of course, each of those sub elements can have more sub elements. There is a
   * requirement that Structs have to map to avro RECORDs and Arrays have to map to avro ARRAYs.
   * In order to build a Struct, we need to build a GenericData.Record based on the schema of that
   * specific field. For Arrays, we need to build a GenericData.Array.
   *
   * In order to avoid a dependency on {@code oracle.sql.TIMESTAMP} and {@code oracle.sql.DATE}
   * we grab the original db field type from the Avro schema to parse these objects. This
   * however does mean that we now have a hard dependency on our specific Avro Schema's.
   *
   * @param sqlObject - the sql Object returned from the Result or a sub element.
   * @param colName - the col name mapping to a field in the avro Schema
   * @param avroSchema - the Avro schema for that specific source or field
   * @return - an Object casted to the expected avro type
   * @throws SQLException
   */
  public Object sqlObjectToAvro(Object sqlObject, String colName, Schema avroSchema) throws SQLException {
    if (sqlObject == null) {
      return null;
    }

    if (sqlObject instanceof String) {
      return sqlObject.toString();
    }

    if (sqlObject instanceof Number) {
      Number number = ((Number) sqlObject);

      Schema primitiveSchema = getChildSchema(avroSchema, colName, ACCEPTABLE_PRIMITIVES);

      if (primitiveSchema.getType().equals(Schema.Type.STRING)) {
        return number.toString();
      }

      if (primitiveSchema.getType().equals(Schema.Type.FLOAT)) {
        return number.floatValue();
      }

      if (primitiveSchema.getType().equals(Schema.Type.INT)) {
        return number.intValue();
      }

      if (primitiveSchema.getType().equals(Schema.Type.LONG)) {
        return number.longValue();
      }

      if (primitiveSchema.getType().equals(Schema.Type.DOUBLE)) {
        return number.doubleValue();
      }
    }

    if (sqlObject instanceof SQLXML) {
      return ((SQLXML) sqlObject).getString();
    }

    if (sqlObject instanceof byte[]) {
      return ByteBuffer.wrap((byte[]) sqlObject);
    }

    if (sqlObject instanceof Date) {
      return ((Date) sqlObject).getTime();
    }

    if (sqlObject instanceof Timestamp) {
      return ((Timestamp) sqlObject).getTime();
    }

    if (sqlObject instanceof Blob) {
      try {
        return blobToBytes((Blob) sqlObject);
      } catch (SQLException e) {
        throw new DatastreamRuntimeException(String.format("Failed to read Blob value for colName: %s", colName), e);
      }
    }

    if (sqlObject instanceof Clob) {
      try {
        return clobToString((Clob) sqlObject);
      } catch (IOException | SQLException e) {
        throw new DatastreamRuntimeException(String.format("Failed to read Clob value for colName: %s", colName), e);
      }
    }

    // STRUCT type refers to a user defined object stored in Oracle.
    // STRUCT types must always be mapped to a GenericData.Record in avro.
    // therefore, we mus recursively build the GenericData.Record based on
    // the avro schema.
    if (sqlObject instanceof java.sql.Struct) {
      // Get the schema for the Record field
      Schema childSchema = getChildSchema(avroSchema, colName, ACCEPTABLE_RECORD);
      // Create a new GenericData Record based on the Avro Schema for this field
      GenericRecord record = new GenericData.Record(childSchema);
      // A Struct can be (and generally is) composed of multiple sub elements
      // including inner Structs and Arrays.
      Struct struct = (Struct) sqlObject;
      Object[] sqlObjectAttributes = struct.getAttributes();

      // ensure that the number of fields in the schema is the same as the database
      if (childSchema.getFields().size() != sqlObjectAttributes.length) {
        String msg = String.format(
            "Avro Schema Mismatch for field: %s on Schema: %s. Avro Schema Field Num: %d, Db Field Num: %d", colName,
            childSchema.getFullName(), childSchema.getFields().size(), sqlObjectAttributes.length);

        throw new DatastreamRuntimeException(msg);
      }

      List<Schema.Field> fields = childSchema.getFields();

      int index = 0;
      for (Schema.Field f : fields) {
        String fieldName = f.name();
        Object dbValue = sqlObjectAttributes[index++];

        Object result = sqlObjectToAvro(dbValue, fieldName, childSchema);
        record.put(fieldName, result);
      }

      return record;
    }


    if (sqlObject instanceof java.sql.Array) {
      Array sqlArray = (Array) sqlObject;
      Object[] sqlObjectArray = (Object[]) sqlArray.getArray();

      Schema fieldSchema = getChildSchema(avroSchema, colName, ACCEPTABLE_COLLECTION);
      GenericData.Array<Object> avroArray = new GenericData.Array<Object>(sqlObjectArray.length, fieldSchema);
      Schema childArraySchema = fieldSchema.getElementType();

      for (Object childArrayObject : sqlObjectArray) {
        Object result = sqlObjectToAvro(childArrayObject, colName, childArraySchema);
        avroArray.add(result);
      }

      return avroArray;
    }

    // handling oracle.sql.DATE
    Schema.Field field = avroSchema.getField(colName);
    String fieldMetaString = field.getProp(META_KEY);
    FieldMetadata fieldMetadata = FieldMetadata.fromString(fieldMetaString);
    Types originalColumnType = fieldMetadata.getDbFieldType();

    if (originalColumnType.equals(Types.DATE)) {
      try {
        return DATE_FORMAT.parse(sqlObject.toString()).getTime();
      } catch (ParseException e) {
        LOG.error("Failed to parse dbFieldType: {} with from sqlObject: {} of type: {}",
            originalColumnType,
            sqlObject.toString(),
            sqlObject.getClass().getName());

        throw new DatastreamRuntimeException(e);
      }
    }

    throw new DatastreamRuntimeException(
        String.format("Cannot convert SQL type: %s for colName: %s value: %s into a AvroType", sqlObject.getClass(),
            colName, sqlObject.toString()));
  }

  /**
   * Return the Schema of a field under the colName. If the underlying field is a
   * UNION type, iterate through it to find the not NULL schema.
   */
  @VisibleForTesting
  static Schema getChildSchema(Schema avroSchema, String colName, List<Schema.Type> acceptable) {
    avroSchema = deUnify(avroSchema);

    // use getField() to get the schema of the colName in respect to the parent schema
    // only used for Struct types.
    Schema fieldSchema = avroSchema.getField(colName) == null ?
        avroSchema : avroSchema.getField(colName).schema();

    fieldSchema = deUnify(fieldSchema);

    if (!acceptable.contains(fieldSchema.getType())) {
      String msg = String.format("Expected Schema Type to be %s, Found: %s. ColName: %s, Schema: %s",
          acceptable.toString(),
          fieldSchema.getType(),
          colName,
          avroSchema.toString(true));

      LOG.error(msg);
      throw new DatastreamRuntimeException(msg);
    }

    return fieldSchema;
  }

  /**
   * Most schema's type are declared as a UNION type in order to support NULL options
   * This function extracts the real type out of the UNION. This function does not modify
   * The actual underlying Schema.
   *
   * @param schema
   * @return Schema - non Union schema
   */
  private static Schema deUnify(Schema schema) {
    while (schema.getType().equals(Schema.Type.UNION)) {
      List<Schema> unionChildSchemas = schema.getTypes();


      if (unionChildSchemas.size() == 1 && unionChildSchemas.get(0).getType().equals(Schema.Type.NULL)) {
        throw new DatastreamRuntimeException("Type for schema cannot only be null: " + schema.toString(true));
      }

      for (Schema unionChildSchema : unionChildSchemas) {
        if (!unionChildSchema.getType().equals(Schema.Type.NULL)) {
          schema = unionChildSchema;
          break;
        }
      }
    }

    return schema;
  }

  /**
   * a java.sql.Clob is a way to store a Character Large Object as a column value in
   * a row of a database table.
   *
   * If the length of the clob is small enough we can simply extract the text by calling
   * {@code ::getSubString()}. However, if longer than {@code Integer.MAX_VALUE} then
   * we need to use the {@code ::getCharacterStream()} api in order to read the whole text
   */
  private static String clobToString(Clob clob) throws SQLException, IOException {
    if (clob == null) {
      return null;
    }

    long length = clob.length();

    if (length <= Integer.MAX_VALUE) {
      return clob.getSubString(1, (int) length);
    }

    Reader reader = null;
    try {
      reader = clob.getCharacterStream();
      StringWriter writer = new StringWriter();
      char[] buffer = new char[1024];
      int n;
      while ((n = reader.read(buffer)) != -1) {
        writer.write(buffer, 0, n);
      }
      return writer.toString();
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          LOG.warn("failed to close reader", e);
        }
      }
    }
  }

  /**
   * A Blob is a Binary Large Object as a column value in a row of
   * a database table
   *
   * We can simply use the {@code ::getBytes()} api in order to
   * extract all the bytes from the Blob
   */
  private static ByteBuffer blobToBytes(Blob blob) throws SQLException {
    if (blob == null) {
      return null;
    }

    byte[] bytes = blob.getBytes(1, (int) blob.length());
    return ByteBuffer.wrap(bytes);
  }

  /**
   * Column names are declared in UPPER_CAMEL but avro field names are LOWER_CAMEL
   * This function converts from UPPER_CAMEL to LOWER_CAMEL while also maintaining a cache
   *
   * @param upperColName - the UPPER_CAMEL column Name from the ResultSet
   * @return the LOWER_CAMEL string
   */
  public String formatColumnName(String upperColName) {
    if (COLUMN_NAME_CACHE.containsKey(upperColName)) {
      return COLUMN_NAME_CACHE.get(upperColName);
    }

    String lowerColName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, upperColName);

    COLUMN_NAME_CACHE.put(upperColName, lowerColName);
    return lowerColName;
  }
}
