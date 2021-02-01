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
 * ScribeParquetAvroConverter handles the conversion of avro to parquet compatible avro schema
 */
public class ScribeParquetAvroConverter {
  private static final Logger LOG = LoggerFactory.getLogger(ScribeParquetAvroConverter.class);
  public static final String SCRIBE_HEADER = "scribeHeader";

  /**
   * Converts field avro schema to parquet compatible format and handles special cases like timestamp, Arrays, Records.
   * @param isNotNullable  Whether the field is required or not
   * @param fieldSchema    Schema of the field
   * @param field          Field
   * @param fieldAssembler FieldAssembler
   * @return SchemaBuilder.FieldAssembler<Schema>
   */
  public static SchemaBuilder.FieldAssembler<Schema> getFieldAssembler(Boolean isNotNullable, Schema fieldSchema, Schema.Field field, SchemaBuilder.FieldAssembler<Schema> fieldAssembler) {
    try {
      String typeName = fieldSchema.getType().name();
      if (typeName.equalsIgnoreCase("ARRAY")) {
        // For list of nested objects
        if (fieldSchema.getElementType().getType().getName().equalsIgnoreCase("RECORD")) {
          Schema elementTypeSchema = generateParquetStructuredAvroSchema(fieldSchema.getElementType());
          fieldAssembler.name(field.name().toLowerCase())
              .doc(field.doc())
              .type()
              .optional()
              .array().items(elementTypeSchema);
          //LOG.info("Getting fieldAssembler for array<record> field: " + field.name());
        } else {
          getFieldAssemblerForRecord(isNotNullable, fieldSchema, field, fieldAssembler);
          //LOG.info("Getting fieldAssembler for array field: " + field.name());
        }
      } else if (typeName.equalsIgnoreCase("MAP")) {
        getFieldAssemblerForRecord(isNotNullable, fieldSchema, field, fieldAssembler);
        //LOG.info("Getting fieldAssembler for map field: " + field.name());
      } else if (typeName.equalsIgnoreCase("RECORD")) {
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
          //LOG.info("**************SCRIBE HEADER *****************" + field.name());
        } else {
          try {
            Schema recordSchema = generateParquetStructuredAvroSchema(fieldSchema);
            getFieldAssemblerForRecord(isNotNullable, recordSchema, field, fieldAssembler);
          } catch (Exception e) {
            LOG.error("Unable to convert nested object avro schema to parquet for field: " + field.name());
          }

          //LOG.info("Getting fieldassembler for record" + field.name());
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
        //LOG.info("Getting fieldassembler for long" + field.name());
      } else {
        getFieldAssemblerForPrimitives(isNotNullable, typeName, field, fieldAssembler);
        //LOG.info("Getting fieldassembler for primitves" + field.name());
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
  public static void getFieldAssemblerForPrimitives(Boolean isNotNullable, String typeName, Schema.Field field, SchemaBuilder.FieldAssembler<Schema> fieldAssembler) {
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
      LOG.error(String.format("Exception in creating primitive fieldbuilder in ScribeParquetAvroConverter: field: %s, type: %s, exception: %s", field.name(), typeName, e));
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
      LOG.error(String.format("Exception in creating record fieldbuilder in ScribeParquetAvroConverter: field: %s, fieldSchema: %s, exception: %s", field.name().toLowerCase(), fieldSchema, e));
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
    LOG.info("generateParquetStructuredAvroSchema schema for event: " + schema.getName());
    try {
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
              fieldAssembler = getFieldAssembler(isNotNullable, typeSchema, field, fieldAssembler);
            }
          }
        } else {
          fieldAssembler = getFieldAssembler(isNotNullable, fieldSchema, field, fieldAssembler);
        }
      }
      parquetAvroSchema = fieldAssembler.endRecord();
      //LOG.info("generateParquetStructuredAvroSchema schema for event: " + schema.getName() + "schema --->" + schema);
      return parquetAvroSchema;
    } catch (Exception e) {
      LOG.error(String.format("Exception in converting avro schema to parquet format in ScribeParquetAvroConverter: Schema: %s, exception: %s", schema.getName(), e));
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
    } catch (Exception e){
      LOG.error(String.format("Exception in getting schema Types in ScribeParquetAvroConverter: field: %s, exception: %s", fieldName, e));
      return null;
    }
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
    LOG.info("Input avrorecord generateParquetStructuredAvroData for: " + avroRecord);
    GenericRecord record = new GenericData.Record(schema);
    LOG.info("Starting generateParquetStructuredAvroData for: " + schema.getName());
    Map<String,String> avroScribeHeaderFieldNamingMap = null;
    GenericRecord scribeHeaderRecord = null;
    if (avroRecord.get(SCRIBE_HEADER) != null) {
      avroScribeHeaderFieldNamingMap = avroRecord.getSchema().getField(SCRIBE_HEADER).schema().getFields().stream().collect(Collectors.toMap(avroScribeHeaderField -> avroScribeHeaderField.name().toLowerCase(), avroScribeHeaderField -> avroScribeHeaderField.name()));
      scribeHeaderRecord = getScribeHeaderParquetData(avroRecord, schema);
      LOG.info("Getting  scribeHeaderRecord: " + scribeHeaderRecord);
    }

    // Get fields from schema
    List<Schema.Field> fields = schema.getFields();
    List<Schema> avroFieldSchemaTypes = null;

    // Map lowercase fieldnames to camelcase fieldnames.
    // The fieldnames(ex: eventname) are in lowercase in parquet schema where as in avrorecord they are camelcase(eventName)
    Map<String,String> avroFieldNamingMap = avroRecord.getSchema().getFields().stream().collect(Collectors.toMap(avroField-> avroField.name().toLowerCase(), avroField-> avroField.name()));


    for (Schema.Field field : fields) {
      String fieldName = avroFieldNamingMap.containsKey(field.name()) ? avroFieldNamingMap.get(field.name()) : field.name();
      try {
        if (fieldName!=null) {
          if (avroRecord.get(fieldName) != null) {
            avroFieldSchemaTypes = getSchemaTypes(fieldName, avroRecord);
          } else if (avroRecord.get(fieldName) == null && ((GenericData.Record) avroRecord).getSchema().getField(fieldName) != null) {
            continue;
          } else if (avroScribeHeaderFieldNamingMap.containsKey(field.name()) && avroRecord.get(SCRIBE_HEADER) != null && avroRecord.get(SCRIBE_HEADER).getClass().getSimpleName().equalsIgnoreCase("Record")) {
            avroFieldSchemaTypes = getSchemaTypes(SCRIBE_HEADER, avroRecord);
          }
        }
      } catch (Exception e) {
        LOG.error(String.format("Exception in getting avro field schema types in ScribeParquetAvroConverter: Schema: %s, field: %s, typeName: %s, exception: %s", schema.getName(), fieldName, e));
      }

      try {
        for (Schema avroTypeSchema : avroFieldSchemaTypes) {
          if (!avroTypeSchema.getType().getName().equalsIgnoreCase("null")) {
            String avroTypeName = avroTypeSchema.getType().name();
            try {

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
                      GenericRecord parquetFormatRecord = generateParquetStructuredAvroData(generateParquetStructuredAvroSchema(avroTypeSchema.getElementType()), nestedObjectRecord);
                      nestedObjectArray.add(parquetFormatRecord);
                    }
                  }
                  record.put(fieldName.toLowerCase(), nestedObjectArray);
                  //LOG.info("array<nestedobject> Conversion to parquet fieldName: " + fieldName + ": " + nestedObjectArray);
                } else {
                  record.put(fieldName.toLowerCase(), avroRecord.get(fieldName));
                  //LOG.info("array<primitive> Conversion to parquet fieldName: " + fieldName + ": " + avroRecord.get(fieldName));
                }
              } else if (avroTypeName.equalsIgnoreCase("LONG")) {
                // For timestamp fields we need to check for logical type in avroschema, to convert to string in parquet
                // for testing with 1.0 events hardcoded eventTimestamp in order to make it human readable
                if (field.name().equalsIgnoreCase("eventTimestamp") ||
                    (avroTypeSchema.getLogicalType() != null && avroTypeSchema.getLogicalType().getName().equalsIgnoreCase("timestamp-millis"))) {
                  // For now converting event timestamp to datetimeFormat
                  // but in 2.0 we still need to figure out how we are handling timestamps
                  String result = null;
                  if (avroRecord.get(fieldName) != null) {
                    Long value = (Long) avroRecord.get(fieldName);
                    result = convertDateTimeToString(value);
                  }
                  record.put(fieldName.toLowerCase(), result);
                  //LOG.info("Timestamp Conversion to parquet fieldName: " + fieldName + ": " + result);
                } else {
                  record.put(fieldName.toLowerCase(), avroRecord.get(fieldName));
                  //LOG.info("Long Conversion to parquet fieldName: " + fieldName + ": " + avroRecord.get(fieldName));
                }
              } else if (avroTypeName.equalsIgnoreCase("RECORD") && avroTypeSchema.getName().equalsIgnoreCase("scribe_header") && scribeHeaderRecord != null) {
                record.put(fieldName.toLowerCase(), scribeHeaderRecord.get(fieldName));
                //LOG.info("Scribe header Conversion to parquet fieldName: " + fieldName + ": " +scribeHeaderRecord.get(fieldName));
              } else if (avroTypeName.equalsIgnoreCase("RECORD")) {
                //convert fields inside nested object to parquet data
                GenericRecord nestedRecord = (GenericRecord) avroRecord.get(fieldName);
                Schema nestedRecordschema = schema.getField(fieldName.toLowerCase()).schema().isUnion() ? schema.getField(fieldName.toLowerCase()).schema().getTypes().get(1) : schema.getField(fieldName.toLowerCase()).schema();
                record.put(fieldName.toLowerCase(), generateParquetStructuredAvroData(nestedRecordschema, nestedRecord));
                //LOG.info("Nested object Conversion to parquet fieldName: " + fieldName + ": " +avroRecord.get(fieldName));
              } else {
                // this is for primitive data types which do not need any conversion
                record.put(fieldName.toLowerCase(), avroRecord.get(fieldName));
                //LOG.info("Primitive Conversion to parquet fieldName: " + fieldName + ": " + avroRecord.get(fieldName));
              }
            } catch (Exception e) {
              LOG.error(String.format("Exception in converting avro field to parquet in ScribeParquetAvroConverter: Schema: %s, field: %s, typeName: %s, exception: %s", schema.getName(), fieldName, avroTypeName, e));
            }
          }
        }
      } catch (NullPointerException e){
        LOG.error(String.format("NPE while converting avro field to parquet in ScribeParquetAvroConverter: Schema: %s, field: %s", schema.getName(), fieldName));
      } catch (Exception e) {
        LOG.error(String.format("Error in converting avro field to parquet in ScribeParquetAvroConverter: Schema: %s, field: %s", schema.getName(), fieldName));
      }
    }
    return record;
  }

  /**
   * Get Parquet formatted scribe header data
   * @param avroRecord
   * @return Parquet-format record
   */
  public static GenericRecord getScribeHeaderParquetData(GenericRecord avroRecord, Schema parquetSchema) {
    LOG.info("Inside getScribeHeaderParquetData");
    Schema scribeHeaderSchema = avroRecord.getSchema().getField(SCRIBE_HEADER).schema();
    LOG.info("Header Schema in getScribeHeaderParquetData " + scribeHeaderSchema.getName());
    Schema scribeHeaderTypeSchema = scribeHeaderSchema.isUnion() ? scribeHeaderSchema.getTypes().get(1) : scribeHeaderSchema;
    LOG.info("Header Type Schema in getScribeHeaderParquetData " + scribeHeaderTypeSchema.getName());
    return generateParquetStructuredAvroData(generateParquetStructuredAvroSchema(scribeHeaderTypeSchema), (GenericRecord) avroRecord.get(SCRIBE_HEADER));
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
