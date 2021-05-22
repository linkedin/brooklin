/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.cloud.storage.io;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides utility methods to convert Avro schema and data into Scribe 2.0 expected format.
 */
public class ScribeParquetAvroConverter {
  private static final Logger LOG = LoggerFactory.getLogger(ScribeParquetAvroConverter.class);
  public static final String SCRIBE_HEADER = "scribeHeader";
  public static final String AVRO_ARRAY = "ARRAY";
  public static final String AVRO_MAP = "MAP";
  public static final String AVRO_RECORD = "RECORD";
  public static final String AVRO_LONG = "LONG";

  /**
   * Converts field avro schema to parquet compatible format and handles special cases like timestamp, Arrays, Records.
   * @param isNotNullable  Whether the field is required or not
   * @param fieldSchema    Schema of the field
   * @param field          Field
   * @param fieldAssembler FieldAssembler
   * @return SchemaBuilder.FieldAssembler<Schema>
   */
  public static SchemaBuilder.FieldAssembler<Schema> getFieldAssembler(Boolean isNotNullable, Schema fieldSchema, Schema.Field field,
                                                                       SchemaBuilder.FieldAssembler<Schema> fieldAssembler) {
    try {
      String typeName = fieldSchema.getType().name();
      if (typeName.equalsIgnoreCase(AVRO_ARRAY)) {
        // For list of nested objects
        if (fieldSchema.getElementType().getType().getName().equalsIgnoreCase(AVRO_RECORD)) {
          Schema elementTypeSchema = generateParquetStructuredAvroSchema(fieldSchema.getElementType());
          fieldAssembler.name(field.name().toLowerCase())
              .doc(field.doc())
              .type()
              .optional()
              .array().items(elementTypeSchema);
        } else {
          getFieldAssemblerForRecord(isNotNullable, fieldSchema, field, fieldAssembler);
        }
      } else if (typeName.equalsIgnoreCase(AVRO_MAP)) {
        getFieldAssemblerForRecord(isNotNullable, fieldSchema, field, fieldAssembler);
      } else if (typeName.equalsIgnoreCase(AVRO_RECORD)) {
        // If the record is of type scribe header, then it is flattened out.
        if (fieldSchema.getName().equalsIgnoreCase("scribe_header")) {
          List<Schema.Field> headerFields = fieldSchema.getFields();
          for (Schema.Field headerField : headerFields) {
            Boolean isHeaderFieldNotNullable = headerField.getObjectProp("isRequired") != null ? (Boolean) headerField.getObjectProp("isRequired") : false;
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
          try {
            Schema recordSchema = generateParquetStructuredAvroSchema(fieldSchema);
            getFieldAssemblerForRecord(isNotNullable, recordSchema, field, fieldAssembler);
          } catch (Exception e) {
            LOG.error("Unable to convert nested object avro schema to parquet for field: " + field.name());
          }
        }
      } else if (typeName.equalsIgnoreCase(AVRO_LONG)) {
        // For timestamp fields we need to add logical type in avroschema, so that it can be converted to string in parquet
        if (fieldSchema.getLogicalType() != null && fieldSchema.getLogicalType().getName().equalsIgnoreCase("timestamp-millis")) {
          getFieldAssemblerForPrimitives(isNotNullable, "string", field, fieldAssembler);
        } else {
          getFieldAssemblerForPrimitives(isNotNullable, typeName, field, fieldAssembler);
        }
      } else {
        getFieldAssemblerForPrimitives(isNotNullable, typeName, field, fieldAssembler);
      }
    } catch (Exception e) {
      LOG.error(String.format("Exception in converting avro field schema to parquet in ScribeParquetAvroConverter: field: %s, exception: %s", field.name(), e));
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
  public static void getFieldAssemblerForPrimitives(Boolean isNotNullable, String typeName, Schema.Field field,
                                                    SchemaBuilder.FieldAssembler<Schema> fieldAssembler) {
    try {
      if (isNotNullable) {
        fieldAssembler.name(field.name().toLowerCase())
            .doc(field.doc())
            .type(typeName.toLowerCase())
            .noDefault();
      } else {
        fieldAssembler.name(field.name().toLowerCase())
            .doc(field.doc())
            .type()
            .optional()
            .type(typeName.toLowerCase());
      }
    } catch (Exception e) {
      LOG.error(String.format("Exception in creating primitive fieldbuilder in ScribeParquetAvroConverter: field: %s," +
          " type: %s, exception: %s", field.name(), typeName, e));
    }
  }

  /**
   * FieldAssembler for record types
   * @param isNotNullable  Whether the field is required or not
   * @param fieldSchema    Schema of the record
   * @param field          Field
   * @param fieldAssembler FieldAssembler
   */
  public static void getFieldAssemblerForRecord(Boolean isNotNullable, Schema fieldSchema, Schema.Field field,
                                                SchemaBuilder.FieldAssembler<Schema> fieldAssembler) {
    try {
      if (isNotNullable) {
        fieldAssembler.name(field.name().toLowerCase())
            .doc(field.doc())
            .type(fieldSchema)
            .noDefault();
      } else {
        fieldAssembler.name(field.name().toLowerCase())
            .doc(field.doc())
            .type()
            .optional()
            .type(fieldSchema);
      }
    } catch (Exception e) {
      LOG.error(String.format("Exception in creating record fieldbuilder in ScribeParquetAvroConverter: field: %s, fieldSchema: %s, exception: %s",
          field.name().toLowerCase(), fieldSchema, e));
    }
  }

  /**
   * This converts the incoming Avro schema to Scribe 2.0 Parquet schema which is used to write records in
   * parquet format.
   *
   * Scribe 2.0 Event Avro schemas supports all Avro primitive types, complex types and Avro logical types
   * (primarily Timestamp and UUID). Includes scribe header nested object defined as a record in all scribe 2.0 events.
   * Only allows adding new fields and doesn’t support field deletion yet.
   *
   * Persisted parquet file schemas supports all Parquet primitive types and parquet logical types(primarily UUID). Scribe Header object
   * is flattened and not nested. Only allows adding new fields and doesn’t support field deletion yet. This parquet schema is used in
   * creating Hive external tables and BigQuery tables
   *
   * @param schema incoming Avro schema
   * @return Schema Persisted Parquet schema
   * @throws Exception
   */
  public static Schema generateParquetStructuredAvroSchema(Schema schema) {
    Schema parquetAvroSchema;
    try {
      SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record(schema.getName())
          .namespace(schema.getNamespace())
          .doc(schema.getDoc())
          .fields();

      List<Schema.Field> fields = schema.getFields();
      for (Schema.Field field : fields) {
        Schema fieldSchema = field.schema();
        Boolean isNotNullable = field.getObjectProp("isRequired") != null ? (Boolean) field.getObjectProp("isRequired") : false;
        if (fieldSchema.isUnion()) {
          for (Schema typeSchema : fieldSchema.getTypes()) {
            if (!typeSchema.getType().getName().equalsIgnoreCase("null")) {
              fieldAssembler = getFieldAssembler(isNotNullable, typeSchema, field, fieldAssembler);
            }
          }
        } else {
          fieldAssembler = getFieldAssembler(isNotNullable, fieldSchema, field, fieldAssembler);
        }
      }
      parquetAvroSchema = fieldAssembler.endRecord();
      return parquetAvroSchema;
    } catch (Exception e) {
      LOG.error(String.format("Exception in converting avro schema to parquet format in ScribeParquetAvroConverter: Schema: %s, exception: %s",
          schema.getName(), e));
      return null;
    }

  }

  /**
   * Gets the schema types from the field avro schema.
   * @param fieldName  the field name
   * @param avroRecord the incoming record
   * @return
   */
  public static List<Schema> getSchemaTypes(String fieldName, GenericRecord avroRecord) {
    try {
      return ((GenericData.Record) avroRecord).getSchema().getField(fieldName).schema().isUnion() ?
          ((GenericData.Record) avroRecord).getSchema().getField(fieldName).schema().getTypes() :
          Arrays.asList(((GenericData.Record) avroRecord).getSchema().getField(fieldName).schema());
    } catch (Exception e) {
      LOG.error(String.format("Exception in getting schema Types in ScribeParquetAvroConverter: field: %s, exception: %s", fieldName, e));
      return null;
    }
  }

  /**
   * The incoming records need to be converted to match the Parquet schema generated using
   * `generateParquetStructuredAvroSchema` function. This function iterates all the fields in the avroRecord
   * {@link ScribeParquetAvroConverter#generateParquetStructuredAvroSchema(Schema)}
   * and converts them to necessary data types to conform to the target Parquet schema.
   *
   * @param schema parquet compatible avro schema
   * @param avroRecord the incoming record
   * @return GenericRecord the record converted to match the new schema
   * @throws Exception
   */
  public static GenericRecord generateParquetStructuredAvroData(Schema schema, GenericRecord avroRecord) {
    GenericRecord record = new GenericData.Record(schema);
    Map<String, String> avroScribeHeaderFieldNamingMap = null;
    GenericRecord scribeHeaderRecord = null;
    if (avroRecord.hasField((SCRIBE_HEADER)) && ((GenericRecord) avroRecord.get(SCRIBE_HEADER) != null)) {
      avroScribeHeaderFieldNamingMap = avroRecord.getSchema().getField(SCRIBE_HEADER).schema().getFields().stream()
          .collect(Collectors.toMap(
              avroScribeHeaderField -> avroScribeHeaderField.name().toLowerCase(), avroScribeHeaderField -> avroScribeHeaderField.name()));
      scribeHeaderRecord = getScribeHeaderParquetData(avroRecord, schema);
    }

    // Get the fields from schema
    List<Schema.Field> fields = schema.getFields();
    List<Schema> avroFieldSchemaTypes = null;

    // Map lowercase fieldnames to camelcase fieldnames.
    // The fieldnames(ex: eventname) are in lowercase in parquet schema where as in avrorecord they are camelcase(eventName)
    Map<String, String> avroFieldNamingMap = avroRecord.getSchema().getFields().stream()
        .collect(Collectors.toMap(avroField-> avroField.name().toLowerCase(), avroField-> avroField.name()));

    for (Schema.Field field : fields) {
      String fieldName = avroFieldNamingMap.containsKey(field.name()) ? avroFieldNamingMap.get(field.name()) : field.name();
      try {
          if (fieldName != null) {
            if (avroRecord.hasField(fieldName) && avroRecord.get(fieldName) != null) {
              avroFieldSchemaTypes = getSchemaTypes(fieldName, avroRecord);
            } else if (!avroRecord.hasField(fieldName) && avroScribeHeaderFieldNamingMap.containsKey(field.name())
                && (avroRecord.hasField((SCRIBE_HEADER)) && (avroRecord.get(SCRIBE_HEADER) != null))
                && avroRecord.get(SCRIBE_HEADER).getClass().getSimpleName().equalsIgnoreCase("Record")) {
              // if field in schema doesn't exists in avroRecord directly but present inside the scribe Header which will be flattened out to top level
              avroFieldSchemaTypes = getSchemaTypes(SCRIBE_HEADER, avroRecord);
            } else if (avroRecord.get(fieldName) == null && ((GenericData.Record) avroRecord).getSchema().getField(fieldName) != null) {
              // if the field doesn't exists in avroRecord, continue to next field
              continue;
            }
          }
      } catch (Exception e) {
        LOG.error(String.format("Exception in getting avro field schema types in ScribeParquetAvroConverter: Schema: %s, field: %s, typeName: %s," +
            " exception: %s", schema, schema.getName(), fieldName, e));
      }

      try {
        for (Schema avroTypeSchema : avroFieldSchemaTypes) {
          if (!avroTypeSchema.getType().getName().equalsIgnoreCase("null")) {
            String avroTypeName = avroTypeSchema.getType().name();
            try {

              if (avroTypeName.equalsIgnoreCase(AVRO_ARRAY)) {
                if (avroTypeSchema.getElementType().getType().getName().equalsIgnoreCase(AVRO_RECORD)) {
                  // get schema of nested obj and generic record
                  List<GenericRecord> nestedObjectArray = new ArrayList<GenericRecord>();
                  // In order to overcome warning: [unchecked] unchecked cast, suppressing it as we are sure that it will be array<record>
                  @SuppressWarnings("unchecked")
                  GenericArray<GenericRecord> array = (GenericArray<GenericRecord>) avroRecord.get(fieldName);
                  if (array != null) {
                    ListIterator<GenericRecord> it = array.listIterator();
                    while (it.hasNext()) {
                      GenericRecord nestedObjectRecord = it.next();
                      GenericRecord parquetFormatRecord =
                          generateParquetStructuredAvroData(generateParquetStructuredAvroSchema(avroTypeSchema.getElementType()), nestedObjectRecord);
                      nestedObjectArray.add(parquetFormatRecord);
                    }
                  }
                  record.put(fieldName.toLowerCase(), nestedObjectArray);
                } else {
                  record.put(fieldName.toLowerCase(), avroRecord.get(fieldName));
                }
              } else if (avroTypeName.equalsIgnoreCase(AVRO_LONG)) {
                // For timestamp fields we need to check for logical type in avroschema, to convert to string in parquet
                if (avroTypeSchema.getLogicalType() != null &&
                        avroTypeSchema.getLogicalType().getName().equalsIgnoreCase("timestamp-millis")) {
                  String result = null;
                  if (avroRecord.get(fieldName) != null) {
                    Long value = (Long) avroRecord.get(fieldName);
                    result = convertDateTimeToString(value);
                  }
                  record.put(fieldName.toLowerCase(), result);
                } else {
                  record.put(fieldName.toLowerCase(), avroRecord.get(fieldName));
                }
              } else if (avroTypeName.equalsIgnoreCase(AVRO_RECORD) &&
                  avroTypeSchema.getName().equalsIgnoreCase("scribe_header") && scribeHeaderRecord != null) {
                record.put(fieldName.toLowerCase(), scribeHeaderRecord.get(fieldName));
              } else if (avroTypeName.equalsIgnoreCase(AVRO_RECORD)) {
                //convert fields inside nested object to parquet data
                GenericRecord nestedRecord = (GenericRecord) avroRecord.get(fieldName);
                Schema nestedRecordschema = schema.getField(fieldName.toLowerCase()).schema().isUnion() ?
                    schema.getField(fieldName.toLowerCase()).schema().getTypes().get(1) :
                    schema.getField(fieldName.toLowerCase()).schema();
                record.put(fieldName.toLowerCase(), generateParquetStructuredAvroData(nestedRecordschema, nestedRecord));
              } else {
                // this is for primitive data types which do not need any conversion
                record.put(fieldName.toLowerCase(), avroRecord.get(fieldName));
              }
            } catch (Exception e) {
              LOG.error(String.format("Exception in converting avro field to parquet in ScribeParquetAvroConverter:" +
                  " Schema: %s, field: %s, typeName: %s, exception: %s", schema.getName(), fieldName, avroTypeName, e));
            }
          }
        }
      } catch (NullPointerException e) {
        LOG.error(String.format("NPE while converting avro field to parquet in ScribeParquetAvroConverter: Schema: %s," +
            " field: %s", schema.getName(), fieldName));
      } catch (Exception e) {
        LOG.error(String.format("Error in converting avro field to parquet in ScribeParquetAvroConverter: Schema: %s," +
            " field: %s", schema.getName(), fieldName));
      }
    }
    LOG.debug(String.format("Parquet record generated for committing: %s", record.toString()));
    return record;
  }

  /**
   * Get Parquet formatted scribe header data
   * @param avroRecord
   * @return Parquet-format record
   */
  public static GenericRecord getScribeHeaderParquetData(GenericRecord avroRecord, Schema parquetSchema) {
    GenericRecord res = null;
    Schema resSchema = null;
    LOG.debug(String.format("Scribe Header Record received for conversion: %s", avroRecord.toString()));
    try {
      Schema scribeHeaderSchema = avroRecord.getSchema().getField(SCRIBE_HEADER).schema();
      Schema scribeHeaderTypeSchema = scribeHeaderSchema.isUnion() ? scribeHeaderSchema.getTypes().get(1) :
          scribeHeaderSchema;

      try {
        resSchema = generateParquetStructuredAvroSchema(scribeHeaderTypeSchema);
      } catch (Exception e) {
        LOG.error(String.format("Error generating parquet schema for scribe header field: %s", e));
      }
      res = generateParquetStructuredAvroData(resSchema, (GenericRecord) avroRecord.get(SCRIBE_HEADER));
    } catch (Exception e){
      LOG.error(String.format("Error generating parquet record data for scribe header field: %s", e));
      LOG.error(avroRecord.toString());
    }

    return res;
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
