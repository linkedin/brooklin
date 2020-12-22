package com.linkedin.datastream.cloud.storage.io;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * ScribeParquetAvroConverter handles the conversion of avro to parquet compatible avro schema
 */
public class ScribeParquetAvroConverter {
    /**
     * Convert an avro string to a String.
     *
     * @param utf8 a Utf8 object
     * @return a String; null if null is provided
     */
    public static String toString(Object utf8) {
        return utf8 == null ? null : utf8.toString();
    }

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
                .namespace("scribe.events")
                .doc(schema.getDoc())
                .fields();

        List<Schema.Field> fields = schema.getFields();
        for (Schema.Field field : fields) {
            Schema fieldSchema = field.schema();
            for (Schema typeSchema : fieldSchema.getTypes()) {
                if (!typeSchema.getType().getName().equalsIgnoreCase("null")) {
                    String typeName = typeSchema.getType().name();
                    if (typeName.equalsIgnoreCase("ARRAY")) {
                        // This is for remaining types i.e list of primitive types
                            fieldAssembler.name(field.name())
                                    .doc(field.doc())
                                    .type()
                                    .optional()
                                    .type(typeSchema);
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

    /**
     * The incoming records need to be converted to match the schema being converted
     * in the above function. This function iterates all the fields in the event class
     * and converts it to necessary data types to conform to the schema in the argument.
     *
     * @param schema parquet compatible avro schema
     * @param event the event name to be written in hdfs
     * @param avroRecord the incoming record
     * @return GenericRecord the record converted to match the new schema
     * @throws Exception
     */
    public static GenericRecord generateParquetStructuredAvroData(Schema schema, String event, GenericRecord avroRecord) {
        GenericRecord record = new GenericData.Record(schema);

        // Get fields from schema
        List<Schema.Field> fields = schema.getFields();
        for (Schema.Field field : fields) {
          String fieldName = field.name();
          List<Schema> avroFieldSchemaTypes = ((GenericData.Record) avroRecord).getSchema().getField(fieldName).schema().getTypes();
          for (Schema avroTypeSchema : avroFieldSchemaTypes) {
              if (!avroTypeSchema.getType().getName().equalsIgnoreCase("null")) {
                String avroTypeName = avroTypeSchema.getType().name();
                if (avroTypeName.equalsIgnoreCase("LONG")) {
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
