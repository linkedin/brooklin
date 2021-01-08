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
    public static GenericRecord generateParquetStructuredAvroData(Schema schema, GenericRecord avroRecord) {
        GenericRecord record = new GenericData.Record(schema);

        // Get fields from schema
        List<Schema.Field> fields = schema.getFields();
        for (Schema.Field field : fields) {
          String fieldName = field.name();
          List<Schema> avroFieldSchemaTypes = ((GenericData.Record) avroRecord).getSchema().getField(fieldName).schema().getTypes();
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
