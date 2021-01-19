package com.linkedin.datastream.cloud.storage.io;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * ScribeParquetAvroConverter handles the conversion of avro to parquet compatible avro schema
 */
public class ScribeParquetAvroConverter {
  public static final String SCRIBE_HEADER = "scribeHeader";

  /**
   * Converts field avro schema to parquet compatible format and handles special cases like timestamp, Arrays, Records.
   * @param isNotNullable  Whether the field is required or not
   * @param fieldSchema    Schema of the field
   * @param field          Field
   * @param fieldAssembler FieldAssembler
   * @return
   */
  public static SchemaBuilder.FieldAssembler<Schema> getFieldAssembler(Boolean isNotNullable, Schema fieldSchema, Schema.Field field, SchemaBuilder.FieldAssembler<Schema> fieldAssembler) {
    String typeName = fieldSchema.getType().name();
    if (typeName.equalsIgnoreCase("ARRAY")) {
      // For list of nested objects
      if (fieldSchema.getElementType().getType().getName().equalsIgnoreCase("RECORD")) {
        Schema elementTypeSchema = generateParquetStructuredAvroSchema(fieldSchema.getElementType());
        fieldAssembler.name(field.name())
            .doc(field.doc())
            .type()
            .optional()
            .array().items(elementTypeSchema);
      } else {
        getFieldAssemblerForRecord(isNotNullable, fieldSchema, field, fieldAssembler);
      }
    } else if (typeName.equalsIgnoreCase("RECORD")) {
      // Replace with header name
      if (fieldSchema.getName().equalsIgnoreCase("scribe_header")) {
        List<Schema.Field> headerFields = fieldSchema.getFields();
        for (Schema.Field headerField : headerFields) {
          Boolean isHeaderFieldNotNullable = headerField.getObjectProp("isRequired") != null ?(Boolean) headerField.getObjectProp("isRequired") : false;
          if (headerField.schema().isUnion()) {
            for (Schema headerSchema : headerField.schema().getTypes()) {
              if (!headerSchema.getType().getName().equalsIgnoreCase("null")) {
                fieldAssembler = getFieldAssembler(isHeaderFieldNotNullable, headerSchema, headerField, fieldAssembler);
              }
            }
          } else {
            fieldAssembler = getFieldAssembler(isHeaderFieldNotNullable, headerField.schema(), headerField, fieldAssembler);
          }
        }
      } else {
        Schema recordSchema = generateParquetStructuredAvroSchema(fieldSchema);
        getFieldAssemblerForRecord(isNotNullable, recordSchema, field, fieldAssembler);
        // For nested objects
      }
    } else if (typeName.equalsIgnoreCase("LONG")) {
      // converting avro long to parquet string type for eventtimestamp only
      // For timestamp fields we need to add logical type in avroschema, so that it can be converted to string in parquet
      if (field.name().equalsIgnoreCase("eventTimestamp") ||
          (fieldSchema.getLogicalType() != null && fieldSchema.getLogicalType().getName().equalsIgnoreCase("timestamp-millis"))) {
        getFieldAssemblerForPrimitives(isNotNullable, "string", field, fieldAssembler);
      } else {
        getFieldAssemblerForPrimitives(isNotNullable, typeName, field, fieldAssembler);
      }
    } else {
      getFieldAssemblerForPrimitives(isNotNullable, typeName, field, fieldAssembler);
    }
    return fieldAssembler;
    }

  /**
   * FieldAssmebler for primitive types
   * @param isNotNullable  Whether the field is required or not
   * @param typeName       Field type
   * @param field          Field
   * @param fieldAssembler FieldAssembler
   */
  public static void getFieldAssemblerForPrimitives(Boolean isNotNullable, String typeName, Schema.Field field, SchemaBuilder.FieldAssembler<Schema> fieldAssembler) {
    if (isNotNullable) {
      fieldAssembler.name(field.name())
          .doc(field.doc())
          .type(typeName.toLowerCase())
          .noDefault();
    } else {
      fieldAssembler.name(field.name())
          .doc(field.doc())
          .type()
          .optional()
          .type(typeName.toLowerCase());
    }
  }

  /**
   * FieldAssembler for record types
   * @param isNotNullable  Whether the field is required or not
   * @param fieldSchema    Schema of the record
   * @param field          Field
   * @param fieldAssembler FieldAssembler
   */
  public static void getFieldAssemblerForRecord(Boolean isNotNullable, Schema fieldSchema, Schema.Field field, SchemaBuilder.FieldAssembler<Schema> fieldAssembler) {
    if (isNotNullable) {
      fieldAssembler.name(field.name())
          .doc(field.doc())
          .type(fieldSchema)
          .noDefault();
    } else {
      fieldAssembler.name(field.name())
          .doc(field.doc())
          .type()
          .optional()
          .type(fieldSchema);
    }
  }

  /**
   * This converts the incoming avro schema to parquet-like avro schema which can be
   * used to write records in parquet format.
   *
   * @param schema incoming avro schema
   * @return Schema parquet compatible avro schema
   * @throws Exception
   */
  public static Schema generateParquetStructuredAvroSchema(Schema schema) {
    Schema parquetAvroSchema;

    SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record(schema.getName())
        .namespace(schema.getNamespace())
        .doc(schema.getDoc())
        .fields();

    List<Schema.Field> fields = schema.getFields();
    for (Schema.Field field : fields) {
      Schema fieldSchema = field.schema();
      Boolean isNotNullable = field.getObjectProp("isRequired") != null ?(Boolean) field.getObjectProp("isRequired") : false;
      if (fieldSchema.isUnion()) {
        for (Schema typeSchema : fieldSchema.getTypes()) {
          if (!typeSchema.getType().getName().equalsIgnoreCase("null")) {
            fieldAssembler = getFieldAssembler(isNotNullable,typeSchema, field, fieldAssembler);
          }
        }
      } else {
            fieldAssembler = getFieldAssembler(isNotNullable,fieldSchema, field, fieldAssembler);
      }

    }
    parquetAvroSchema = fieldAssembler.endRecord();
    return parquetAvroSchema;
  }

  /**
   * Gets the schema types from the field avro schema.
   * @param fieldName  the field name
   * @param avroRecord the incoming record
   * @return
   */
  public static List<Schema> getSchemaTypes(String fieldName, GenericRecord avroRecord) {
    return avroRecord.getSchema().getField(fieldName).schema().isUnion() ?
        avroRecord.getSchema().getField(fieldName).schema().getTypes() :
        Arrays.asList(avroRecord.getSchema().getField(fieldName).schema());
  }

  /**
   * The incoming records need to be converted to match the schema being converted
   * in the `generateParquetStructuredAvroSchema` function. This function iterates all the fields in the event schema
   * and converts it to necessary data types to conform to the schema in the argument.
   *
   * @param schema parquet compatible avro schema
   * @param avroRecord the incoming record
   * @return GenericRecord the record converted to match the new schema
   * @throws Exception
   */
  public static GenericRecord generateParquetStructuredAvroData(Schema schema, GenericRecord avroRecord) {
      GenericRecord record = new GenericData.Record(schema);
      GenericRecord scribeHeaderRecord = null;
      if (avroRecord.get(SCRIBE_HEADER) != null) {
        scribeHeaderRecord = getScribeHeaderParquetData(avroRecord);
      }

      // Get fields from schema
      List<Schema.Field> fields = schema.getFields();
      List<Schema> avroFieldSchemaTypes = null;
      for (Schema.Field field : fields) {
        String fieldName = field.name();
        // replace with scribe header fieldName
        if (avroRecord.get(fieldName) != null) {
          avroFieldSchemaTypes = getSchemaTypes(fieldName, avroRecord);
        } else if (avroRecord.get(fieldName) == null && avroRecord.getSchema().getField(fieldName) != null){
          continue;
        } else if(avroRecord.get(SCRIBE_HEADER) != null && avroRecord.get(SCRIBE_HEADER).getClass().getSimpleName().equalsIgnoreCase("Record")){
          avroFieldSchemaTypes = getSchemaTypes(SCRIBE_HEADER, avroRecord);
        }
        for (Schema avroTypeSchema : avroFieldSchemaTypes) {
            if (!avroTypeSchema.getType().getName().equalsIgnoreCase("null")) {
              String avroTypeName = avroTypeSchema.getType().name();
              if (avroTypeName.equalsIgnoreCase("ARRAY")) {
                if (avroTypeSchema.getElementType().getType().getName().equalsIgnoreCase("RECORD")) {
                  // get schema of nested obj and generic record
                  List<GenericRecord> nestedObjectArray = new ArrayList<GenericRecord>();
                  // In order to overcome warning: [unchecked] unchecked cast, suppressing it as we are sure that it will be array<record>
                  @SuppressWarnings("unchecked")
                  GenericArray<GenericRecord> array = (GenericArray<GenericRecord>) avroRecord.get(fieldName);
                  if (array != null) {
                    ListIterator<GenericRecord> it = array.listIterator();
                    while (it.hasNext()) {
                      GenericRecord nestedObjectRecord = it.next();
                      GenericRecord parquetFormatRecord = generateParquetStructuredAvroData(avroTypeSchema.getElementType(), nestedObjectRecord);
                      nestedObjectArray.add(parquetFormatRecord);
                    }
                  }
                  record.put(fieldName, nestedObjectArray);
                } else {
                  record.put(fieldName, avroRecord.get(fieldName));
                }
              } else if (avroTypeName.equalsIgnoreCase("LONG")) {
                // For timestamp fields we need to check for logical type in avroschema, to convert to string in parquet
                // for testing with 1.0 events hardcoded eventTimestamp in order to make it human readable
                if (field.name().equalsIgnoreCase("eventTimestamp") ||
                    (avroTypeSchema.getLogicalType() != null &&
                        avroTypeSchema.getLogicalType().getName().equalsIgnoreCase("timestamp-millis"))) {
                  String result = null;
                  if (avroRecord.get(fieldName) != null) {
                    Long value = (Long) avroRecord.get(fieldName);
                    result = convertDateTimeToString(value);
                  }
                  record.put(fieldName, result);
                } else {
                  record.put(fieldName, avroRecord.get(fieldName));
                }
                //Replace with header name for 2.0
              } else if (avroTypeName.equalsIgnoreCase("RECORD") && avroTypeSchema.getName().equalsIgnoreCase("scribe_header") && scribeHeaderRecord != null) {
                record.put(fieldName, scribeHeaderRecord.get(fieldName));
              } else if (avroTypeName.equalsIgnoreCase("RECORD")) {
                //convert fields inside nested object to parquet data
                GenericRecord nestedRecord = (GenericRecord) avroRecord.get(fieldName);
                Schema nestedRecordschema = schema.getField(fieldName).schema().isUnion() ? schema.getField(fieldName).schema().getTypes().get(1): schema.getField(fieldName).schema();
                record.put(fieldName, generateParquetStructuredAvroData(nestedRecordschema, nestedRecord));
              } else {
                // this is for primitive data types which do not need any conversion
                record.put(fieldName, avroRecord.get(fieldName));
              }

            }
          }
      }
      return record;
  }

  /**
   * Get Parquet formatted scribe header data
   * @param avroRecord
   * @return Parquet-format record
   */
  public static GenericRecord getScribeHeaderParquetData(GenericRecord avroRecord) {
    Schema scribeHeaderSchema = avroRecord.getSchema().getField(SCRIBE_HEADER).schema();
    Schema scribeHeaderTypeSchema = scribeHeaderSchema.isUnion() ? scribeHeaderSchema.getTypes().get(1) : scribeHeaderSchema;

    return generateParquetStructuredAvroData(scribeHeaderTypeSchema, (GenericRecord) avroRecord.get(SCRIBE_HEADER));
  }

  /**
   * Convert long epoch timestamp value to String date time format
   * @param value long epoch timestamp
   * @return String datetime format
   */
  private static String convertDateTimeToString(Long value) {
    DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS ZZ");
    String date = format.format(new Date(value));
    return date;
  }

}
