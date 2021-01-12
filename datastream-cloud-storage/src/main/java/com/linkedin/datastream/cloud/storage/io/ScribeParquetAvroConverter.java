package com.linkedin.datastream.cloud.storage.io;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.serialization.record.RecordSchema;

/**
 * ScribeParquetAvroConverter handles the conversion of avro to parquet compatible avro schema
 */
public class ScribeParquetAvroConverter {

    /**
     * This converts the incoming avro schema to parquet-like avro schema which can be
     * used to write records in parquet format. The fields in the records previously
     * written in binary format will be converted to string. This eliminates the use of
     * creating views in the HIVE.
     *
     * @param schema incoming avro schema
     * @param eventName the event name to be written in hdfs
     * @return Schema parquet compatible avro schema
     * @throws Exception
     */
    public static Schema generateParquetStructuredAvroSchema(Schema schema, String eventName) {
        Schema parquetAvroSchema;

        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record(eventName)
                .namespace(schema.getNamespace())
                .doc(schema.getDoc())
                .fields();

        List<Schema.Field> fields = schema.getFields();
        for (Schema.Field field : fields) {
          Schema fieldSchema = field.schema();
            for (Schema typeSchema : fieldSchema.getTypes()) {
                if (!typeSchema.getType().getName().equalsIgnoreCase("null")) {
                    String typeName = typeSchema.getType().name();
                    if (typeName.equalsIgnoreCase("ARRAY")) {
                      // For list of nested objects
                      if (typeSchema.getElementType().getType().getName().equalsIgnoreCase("RECORD")) {
                        Schema elementTypeSchema = generateParquetStructuredAvroSchema(typeSchema.getElementType(), typeSchema.getElementType().getName());
                        fieldAssembler.name(field.name())
                            .doc(field.doc())
                            .type()
                            .optional()
                            .array().items(elementTypeSchema);
                      } else {
                        fieldAssembler.name(field.name())
                            .doc(field.doc())
                            .type()
                            .optional()
                            .type(typeSchema);
                      }
                    } else if (typeName.equalsIgnoreCase("RECORD")) {
                      // For nested objects
                      Schema recordSchema = generateParquetStructuredAvroSchema(typeSchema, typeSchema.getName());
                      fieldAssembler.name(field.name())
                          .doc(field.doc())
                          .type()
                          .optional()
                          .type(recordSchema);
                    } else if (typeName.equalsIgnoreCase("LONG")) {
                      // converting avro long to parquet string type for eventtimestamp only
                      // for testing scribe 1.0 events as there won't be fields of type timestamp/ Datetime

                      // For timestamp fields we need to add logical type in avroschema, so that it can be converted to string in parquet
                      if (field.name().equalsIgnoreCase("eventTimestamp") ||
                          (typeSchema.getLogicalType() != null && typeSchema.getLogicalType().getName().equalsIgnoreCase("timestamp-millis"))) {
                        fieldAssembler.optionalString(field.name());
                      } else {
                        fieldAssembler.name(field.name())
                            .doc(field.doc())
                            .type()
                            .optional()
                            .type(typeSchema);
                      }
                    } else {
                        // Fields with below data types need not be converted and hence written as is
                        fieldAssembler.name(field.name())
                                .doc(field.doc())
                                .type()
                                .optional()
                                .type(typeSchema);
                    }
                }
            }
        }
        parquetAvroSchema = fieldAssembler.endRecord();
        return parquetAvroSchema;
    }

  public static SchemaBuilder.FieldAssembler<Schema> getFieldAssembler(Schema fieldSchema, Schema.Field field, SchemaBuilder.FieldAssembler<Schema> fieldAssembler) {
    String typeName = fieldSchema.getType().name();
    Boolean isNotNullable = field.getObjectProp("isRequired") != null ?(Boolean) field.getObjectProp("isRequired") : false;
    if (typeName.equalsIgnoreCase("ARRAY")) {
      // For list of nested objects
      if (fieldSchema.getElementType().getType().getName().equalsIgnoreCase("RECORD")) {
        Schema elementTypeSchema = generateParquetStructuredAvroSchema(fieldSchema.getElementType(), fieldSchema.getElementType().getName());
        fieldAssembler.name(field.name())
            .doc(field.doc())
            .type()
            .optional()
            .array().items(elementTypeSchema);
      } else {
        getFieldAssemblerForRecord(isNotNullable, fieldSchema, field, fieldAssembler);
      }
    } else if (typeName.equalsIgnoreCase("RECORD")) {
      //Schema recordSchema = generateFlattenedHeaderParquetStructuredAvroSchema(fieldSchema, fieldSchema.getName());

      // For testing using ProductManufacturerPart, replace with header name in prod
      if (fieldSchema.getName().equalsIgnoreCase("ScribeHeader") || fieldSchema.getName().equalsIgnoreCase("ProductManufacturerPart")) {
        List<Schema.Field> headerFields = fieldSchema.getFields();
        for (Schema.Field headerField : headerFields) {
          if (headerField.schema().isUnion()) {
            for (Schema headerSchema : headerField.schema().getTypes()) {
              if (!headerSchema.getType().getName().equalsIgnoreCase("null")) {
                fieldAssembler = getFieldAssemblerForNullableField(headerSchema, headerField, fieldAssembler);
              }
            }
          } else {
            fieldAssembler = getFieldAssembler(headerField.schema(), headerField, fieldAssembler);
          }
          //getFieldAssemblerForPrimitives(isNotNullable, headerField.schema().getType().getName(), headerField, fieldAssembler);
        }
      } else {
        Schema recordSchema = generateFlattenedHeaderParquetStructuredAvroSchema(fieldSchema, fieldSchema.getName());
        getFieldAssemblerForRecord(isNotNullable, recordSchema, field, fieldAssembler);
        // For nested objects
      }
    } else if (typeName.equalsIgnoreCase("LONG")) {
      // converting avro long to parquet string type for eventtimestamp only
      // for testing scribe 1.0 events as there won't be fields of type timestamp/ Datetime

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

    public static void getFieldAssemblerForPrimitives(Boolean isNotNullable, String typeName, Schema.Field field, SchemaBuilder.FieldAssembler<Schema> fieldAssembler) {
      if (isNotNullable) {
        // Fields with below data types need not be converted and hence written as is
        fieldAssembler.name(field.name())
            .doc(field.doc())
            .type(typeName.toLowerCase())
            .noDefault();
      } else {
        // Fields with below data types need not be converted and hence written as is
        fieldAssembler.name(field.name())
            .doc(field.doc())
            .type()
            .optional()
            .type(typeName.toLowerCase());
      }

    }

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

    public static SchemaBuilder.FieldAssembler<Schema> getFieldAssemblerForNullableField(Schema typeSchema, Schema.Field field, SchemaBuilder.FieldAssembler<Schema> fieldAssembler) {
      String typeName = typeSchema.getType().name();
      if (typeName.equalsIgnoreCase("ARRAY")) {
        // For list of nested objects
        if (typeSchema.getElementType().getType().getName().equalsIgnoreCase("RECORD")) {
          Schema elementTypeSchema = generateParquetStructuredAvroSchema(typeSchema.getElementType(), typeSchema.getElementType().getName());
          fieldAssembler.name(field.name())
              .doc(field.doc())
              .type()
              .optional()
              .array().items(elementTypeSchema);
        } else {
          getFieldAssemblerForRecord(false, typeSchema, field, fieldAssembler);
        }
      } else if (typeName.equalsIgnoreCase("RECORD")) {
        // For testing using ProductManufacturerPart, replace with header name in prod
        if (typeSchema.getName().equalsIgnoreCase("ScribeHeader") || typeSchema.getName().equalsIgnoreCase("ProductManufacturerPart")) {
          List<Schema.Field> headerFields = typeSchema.getFields();
          for (Schema.Field headerField : headerFields) {
            if (headerField.schema().isUnion()) {
              for (Schema headerSchema : headerField.schema().getTypes()) {
                if (!headerSchema.getType().getName().equalsIgnoreCase("null")) {
                  fieldAssembler = getFieldAssemblerForNullableField(headerSchema, headerField, fieldAssembler);
                }
              }
            } else {
              fieldAssembler = getFieldAssembler(headerField.schema(), headerField, fieldAssembler);
            }

            //getFieldAssemblerForRecord(false, headerField.schema().getTypes().get(1), headerField, fieldAssembler);
          }
        } else {
          // For nested objects
          Schema recordSchema = generateFlattenedHeaderParquetStructuredAvroSchema(typeSchema, typeSchema.getName());
          getFieldAssemblerForRecord(false, recordSchema, field, fieldAssembler);
        }
      } else if (typeName.equalsIgnoreCase("LONG")) {
        // converting avro long to parquet string type for eventtimestamp only
        // for testing scribe 1.0 events as there won't be fields of type timestamp/ Datetime

        // For timestamp fields we need to add logical type in avroschema, so that it can be converted to string in parquet
        if (field.name().equalsIgnoreCase("eventTimestamp") ||
            (typeSchema.getLogicalType() != null && typeSchema.getLogicalType().getName().equalsIgnoreCase("timestamp-millis"))) {
          getFieldAssemblerForPrimitives(false, "string", field, fieldAssembler);
          //fieldAssembler.optionalString(field.name());
        } else {
          getFieldAssemblerForPrimitives(false, typeName, field, fieldAssembler);
        }
      } else {
        // Fields with below data types need not be converted and hence written as is
        getFieldAssemblerForPrimitives(false, typeName, field, fieldAssembler);
      }
      return fieldAssembler;
    }

  /**
   *
   * @param schema
   * @param eventName
   * @return
   */
  public static Schema generateFlattenedHeaderParquetStructuredAvroSchema(Schema schema, String eventName) {
    Schema parquetAvroSchema;

    SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record(eventName)
        .namespace(schema.getNamespace())
        .doc(schema.getDoc())
        .fields();

    List<Schema.Field> fields = schema.getFields();
    for (Schema.Field field : fields) {
      Schema fieldSchema = field.schema();
      if (fieldSchema.isUnion()) {
        for (Schema typeSchema : fieldSchema.getTypes()) {
          if (!typeSchema.getType().getName().equalsIgnoreCase("null")) {
            fieldAssembler = getFieldAssemblerForNullableField(typeSchema, field, fieldAssembler);
          }
        }
      } else {
            fieldAssembler = getFieldAssembler(fieldSchema, field, fieldAssembler);
      }

    }
    parquetAvroSchema = fieldAssembler.endRecord();
    return parquetAvroSchema;
  }

    /**
     * The incoming records need to be converted to match the schema being converted
     * in the above function. This function iterates all the fields in the event class
     * and converts it to necessary data types to conform to the schema in the argument.
     *
     * @param schema parquet compatible avro schema
     * @param avroRecord the incoming record
     * @return GenericRecord the record converted to match the new schema
     * @throws Exception
     */
    public static GenericRecord generateParquetStructuredAvroData(Schema schema, GenericRecord avroRecord) {
        GenericRecord record = new GenericData.Record(schema);

        // Get fields from schema
        List<Schema.Field> fields = schema.getFields();
      List<Schema> avroFieldSchemaTypes = null;
        for (Schema.Field field : fields) {
          String fieldName = field.name();
          // replace with scribe header fieldName
          if (avroRecord.get(fieldName) != null) {
            avroFieldSchemaTypes = ((GenericData.Record) avroRecord).getSchema().getField(fieldName).schema().getTypes();
          } else if (avroRecord.get(fieldName) == null && ((GenericData.Record) avroRecord).getSchema().getField(fieldName) != null){
            continue;
          } else if(avroRecord.get("manufacturerPart") != null && avroRecord.get("manufacturerPart").getClass().getSimpleName().equalsIgnoreCase("Record")){
            avroFieldSchemaTypes = ((GenericData.Record) avroRecord).getSchema().getField("manufacturerPart").schema().getTypes();
          }
          for (Schema avroTypeSchema : avroFieldSchemaTypes) {
              if (!avroTypeSchema.getType().getName().equalsIgnoreCase("null")) {
                String avroTypeName = avroTypeSchema.getType().name();
                if (avroTypeName.equalsIgnoreCase("ARRAY")) {
                  if (avroTypeSchema.getElementType().getType().getName().equalsIgnoreCase("RECORD")) {
                    // get schema of nested obj and generic record
                    List<GenericRecord> nestedObjectArray = new ArrayList<GenericRecord>();
                    GenericArray array = (GenericArray) avroRecord.get(fieldName);
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
                    // For now converting event timestamp to datetimeFormat
                    // but in 2.0 we still need to figure out how we are handling timestamps
                    String result = null;
                    if (avroRecord.get(fieldName) != null) {
                      Long value = (Long) avroRecord.get(fieldName);
                      result = convertDateTimeToString(value);
                    }
                    record.put(fieldName, result);
                  } else {
                    // this is for primitive data types which do not need any conversion
                    record.put(fieldName, avroRecord.get(fieldName));
                  }
                  // Using ProductManufacturerPart to test for 1.0 event, will replace with header name for 2.0
                } else if (avroTypeName.equalsIgnoreCase("RECORD") && avroTypeSchema.getName().equalsIgnoreCase("ProductManufacturerPart")) {
                  GenericRecord nestedRecord = (GenericRecord) avroRecord.get("manufacturerPart");
                  record.put(fieldName, nestedRecord.get(fieldName));
                } else {
                  record.put(fieldName, avroRecord.get(fieldName));
                }

              }
            }
        }
        return record;
    }

  private static String convertDateTimeToString(Long value) {
    DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS ZZ");
    String date = format.format(new Date(value));
    return date;
  }

}
