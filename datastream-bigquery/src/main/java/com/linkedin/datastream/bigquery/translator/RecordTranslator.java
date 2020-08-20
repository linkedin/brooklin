/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery.translator;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.nio.ByteBuffer;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.InsertAllRequest;

/**
 * This class translates given avro record into BQ row object.
 */
public class RecordTranslator {

    private static boolean isPrimitiveType(Schema.Type type) {
        return (type == Schema.Type.BOOLEAN ||
                type == Schema.Type.INT ||
                type == Schema.Type.LONG ||
                type == Schema.Type.FLOAT ||
                type == Schema.Type.DOUBLE ||
                type == Schema.Type.BYTES ||
                type == Schema.Type.STRING);
    }

    private static boolean isNamedTypeByRecord(Object record) {
        return record instanceof GenericRecord || record instanceof GenericData.EnumSymbol || record instanceof GenericData.Fixed;
    }

    private static boolean isPrimitiveTypeByRecord(Object record) {
        return record instanceof Boolean ||
                record instanceof Integer ||
                record instanceof Long ||
                record instanceof Float ||
                record instanceof Double ||
                record instanceof ByteBuffer ||
                record instanceof String || record instanceof Utf8;
    }

    private static boolean hasOnlyOneNonNullType(Schema schema) {
        return schema.getType() == Schema.Type.UNION &&
                (schema.getTypes().size() == 2 &&
                        (schema.getTypes().get(0).getType() == Schema.Type.NULL ||
                                schema.getTypes().get(1).getType() == Schema.Type.NULL));
    }

    private static Schema getTheOnlyNonNullType(Schema schema) {
       return (schema.getTypes().get(0).getType() != Schema.Type.NULL) ?
               schema.getTypes().get(0) : schema.getTypes().get(1);
    }

    private static Schema findUnionSubSchema(Schema unionSchema, Object record) {

        if (isNamedTypeByRecord(record)) {
            return ((GenericContainer) record).getSchema();
        }

        Schema.Type type = null;
        if (record instanceof Boolean) {
            type = Schema.Type.BOOLEAN;
        } else if (record instanceof Integer) {
            type = Schema.Type.INT;
        } else if (record instanceof Long) {
            type = Schema.Type.LONG;
        } else if (record instanceof Float) {
            type = Schema.Type.FLOAT;
        } else if (record instanceof Double) {
            type = Schema.Type.DOUBLE;
        } else if (record instanceof ByteBuffer) {
            type = Schema.Type.BYTES;
        } else if (record instanceof String || record instanceof Utf8) {
            type = Schema.Type.STRING;
        } else if (record instanceof GenericArray) {
            type = Schema.Type.ARRAY;
        } else if (record instanceof  Map) {
            type = Schema.Type.MAP;
        }

        for (Schema schema: unionSchema.getTypes()) {
            if (schema.getType() == type) {
                return schema;
            }
        }

        return null;
    }

    private static String sanitizeName(String name) {
        return name.toLowerCase().replace("-", "_");
    }

    private static Object translatePrimitiveTypeObject(Object record, Schema avroSchema) {
        return translatePrimitiveTypeObject(record, avroSchema, "").getValue();
    }

    private static Object translateEnumTypeObject(Object record) {
        return translateEnumTypeObject(record, "").getValue();
    }

    private static Object translateFixedTypeObject(Object record, Schema avroSchema) {
        return translateFixedTypeObject(record, avroSchema, "").getValue();
    }

    private static Object translateArrayTypeObject(Object record, Schema avroSchema) {
        return translateArrayTypeObject(record, avroSchema, "").getValue();
    }

    private static Object translateUnionTypeObject(Object record, Schema avroSchema) {
        return translateUnionTypeObject(record, avroSchema, "").getValue();
    }

    private static Object translateMapTypeObject(Object record, Schema avroSchema) {
        return translateMapTypeObject(record, avroSchema, "").getValue();
    }

    private static Map.Entry<String, Object> translatePrimitiveTypeObject(Object record, Schema avroSchema, String name) {
        Map.Entry<String, Object> result = new AbstractMap.SimpleEntry<>(name, null);
        switch (avroSchema.getType()) {
            case STRING:
                if (LogicalTypeIdentifier.isTimeType(avroSchema)) {
                    result = new AbstractMap.SimpleEntry<>(name, LogicalTypeTranslator.translateTimeType(String.valueOf(record), avroSchema));
                } else if (LogicalTypeIdentifier.isTimestampType(avroSchema)) {
                    result = new AbstractMap.SimpleEntry<>(name, LogicalTypeTranslator.translateTimestampType(String.valueOf(record), avroSchema));
                } else {
                    result = new AbstractMap.SimpleEntry<>(name, String.valueOf(record));
                }
                break;
            case FLOAT:
            case DOUBLE:
                result = new AbstractMap.SimpleEntry<>(name, record);
                break;
            case INT:
                if (LogicalTypeIdentifier.isDateType(avroSchema)) {
                    result = new AbstractMap.SimpleEntry<>(name, LogicalTypeTranslator.translateDateType((Integer) record));
                    break;
                } else if (LogicalTypeIdentifier.isTimeType(avroSchema)) {
                    result = new AbstractMap.SimpleEntry<>(name, LogicalTypeTranslator.translateTimeType((Integer) record, avroSchema));
                    break;
                }
            case LONG:
                if (LogicalTypeIdentifier.isTimeType(avroSchema)) {
                    result = new AbstractMap.SimpleEntry<>(name, LogicalTypeTranslator.translateTimeType((Long) record, avroSchema));
                } else if (LogicalTypeIdentifier.isTimestampType(avroSchema)) {
                    result = new AbstractMap.SimpleEntry<>(name, LogicalTypeTranslator.translateTimestampType((Long) record, avroSchema));
                } else if (LogicalTypeIdentifier.isDatetimeType(avroSchema)) {
                    result = new AbstractMap.SimpleEntry<>(name, LogicalTypeTranslator.translateDatetimeType((Long) record, avroSchema));
                } else {
                    result = new AbstractMap.SimpleEntry<>(name, record);
                }
                break;
            case BOOLEAN:
                result = new AbstractMap.SimpleEntry<>(name, record);
                break;
            case BYTES:
                if (LogicalTypeIdentifier.isDecimalType(avroSchema)) {
                    final LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) avroSchema.getLogicalType();
                    result = new AbstractMap.SimpleEntry<>(name,
                            new BigDecimal(new BigInteger(((ByteBuffer) record).array()), decimalType.getScale()));
                } else {
                    result = new AbstractMap.SimpleEntry<>(name,
                            Base64.getEncoder().encodeToString(((ByteBuffer) record).array()));
                }
                break;
            default:
                return result;
        }
        return result;
    }

    private static Map.Entry<String, Object> translateEnumTypeObject(Object record, String name) {
        return new AbstractMap.SimpleEntry<>(name, String.valueOf(record));
    }

    private static Map.Entry<String, Object> translateFixedTypeObject(Object record, Schema avroSchema, String name) {
        if (LogicalTypeIdentifier.isDecimalType(avroSchema)) {
            LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) avroSchema.getLogicalType();
            return new AbstractMap.SimpleEntry<>(name, new BigDecimal(new BigInteger(((GenericFixed) record).bytes()), decimalType.getScale()));
        } else {
            return new AbstractMap.SimpleEntry<>(name, Base64.getEncoder().encodeToString(((GenericFixed) record).bytes()));
        }
    }

    private static Map.Entry<String, Object> translateUnionTypeObject(Object record, Schema avroSchema, String name) {

        Map.Entry<String, Object> result = new AbstractMap.SimpleEntry<>(name, null);

        if (record == null) {
            return result;
        }

        if (hasOnlyOneNonNullType(avroSchema)) {
            Schema typeSchema = getTheOnlyNonNullType(avroSchema);

            if (isPrimitiveType(typeSchema.getType())) {
                result = translatePrimitiveTypeObject(record, typeSchema, name);
            } else if (typeSchema.getType() == Schema.Type.RECORD) {
                result = new AbstractMap.SimpleEntry<>(name, translateRecord((GenericData.Record) record));
            } else if (typeSchema.getType() == Schema.Type.FIXED) {
                result = translateFixedTypeObject(record, typeSchema, typeSchema.getName());
            } else if (typeSchema.getType() == Schema.Type.MAP) {
                result = translateMapTypeObject(record, typeSchema, name);
            } else if (typeSchema.getType() == Schema.Type.ENUM) {
                result = translateEnumTypeObject(record, name);
            } else if (typeSchema.getType() == Schema.Type.ARRAY) {
                result = translateArrayTypeObject(record, typeSchema, name);
            }

        } else {

            TableRow fieldVaules = new TableRow();

            if (isPrimitiveTypeByRecord(record) || record instanceof GenericData.Fixed) {
                Schema subTypeSchema = findUnionSubSchema(avroSchema, record);
                if (subTypeSchema != null) {
                    LogicalType logicalType = subTypeSchema.getLogicalType();
                    String suffix = (logicalType == null || logicalType.getName().equals("uuid")) ? "_value" :
                            "_" + sanitizeName(logicalType.getName()) + "_value";
                    if (isPrimitiveTypeByRecord(record)) {
                        fieldVaules.put(subTypeSchema.getType().getName().toLowerCase() + suffix,
                                translatePrimitiveTypeObject(record, subTypeSchema));
                    } else {
                        fieldVaules.put(subTypeSchema.getType().getName().toLowerCase() + suffix,
                                translateFixedTypeObject(record, subTypeSchema));
                    }
                }
            } else if (record instanceof GenericArray) {
                Schema subTypeSchema = findUnionSubSchema(avroSchema, record);
                if (subTypeSchema != null) {
                    LogicalType logicalType = subTypeSchema.getElementType().getLogicalType();

                    String suffix = "_" + subTypeSchema.getElementType().getName().toLowerCase() +
                            ((logicalType == null) ? "_value" : "_" + sanitizeName(logicalType.getName()) + "_value");

                    fieldVaules.put(subTypeSchema.getType().getName().toLowerCase() + suffix,
                            translateArrayTypeObject(record, subTypeSchema));
                }
            } else if (record instanceof Map) {
                Schema subTypeSchema = findUnionSubSchema(avroSchema, record);
                if (subTypeSchema != null) {
                    LogicalType logicalType = subTypeSchema.getValueType().getLogicalType();

                    String suffix = "_" + subTypeSchema.getValueType().getName().toLowerCase() +
                            ((logicalType == null) ? "_value" : "_" + sanitizeName(logicalType.getName()) + "_value");

                    fieldVaules.put(subTypeSchema.getType().getName().toLowerCase() + suffix,
                            translateMapTypeObject(record, subTypeSchema));
                }
            } else if (record instanceof GenericRecord) {
                GenericData.Record rec = (GenericData.Record) record;
                fieldVaules.put(rec.getSchema().getName().toLowerCase() + "_value", translateRecord(rec));
            } else if (record instanceof GenericData.EnumSymbol) {
                GenericData.EnumSymbol rec = (GenericData.EnumSymbol) record;
                fieldVaules.put(rec.getSchema().getName().toLowerCase() + "_value", translateEnumTypeObject(record));
            }

            result = new AbstractMap.SimpleEntry<>(name, fieldVaules);
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    private static Map.Entry<String, Object> translateMapTypeObject(Object record, Schema avroSchema, String name) {
        Map.Entry<String, Object> result = new AbstractMap.SimpleEntry<>(name, null);
        List<Map<String, Object>> subRecords = new ArrayList<>();

        if (isPrimitiveType(avroSchema.getValueType().getType())) {
            Map<String, Object> map = (Map<String, Object>) record;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                TableRow subRecord = new TableRow();
                subRecord.put("key", String.valueOf(entry.getKey()));
                subRecord.put("value", translatePrimitiveTypeObject(entry.getValue(), avroSchema.getValueType()));
                subRecords.add(subRecord);
            }
            result = new AbstractMap.SimpleEntry<>(name, subRecords);
        } else if (avroSchema.getValueType().getType() == Schema.Type.ARRAY) {
            Map<String, Object> map = (Map<String, Object>) record;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                TableRow subRecord = new TableRow();
                subRecord.put("key", String.valueOf(entry.getKey()));
                subRecord.put("value", translateArrayTypeObject(entry.getValue(), avroSchema.getValueType()));
                subRecords.add(subRecord);
            }
            result = new AbstractMap.SimpleEntry<>(name, subRecords);
        } else if (avroSchema.getValueType().getType() == Schema.Type.RECORD) {
            Map<String, Object> map = (Map<String, Object>) record;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                TableRow subRecord = new TableRow();
                subRecord.put("key", String.valueOf(entry.getKey()));
                subRecord.put("value", translateRecord((GenericData.Record) entry.getValue()));
                subRecords.add(subRecord);
            }
            result = new AbstractMap.SimpleEntry<>(name, subRecords);
        } else if (avroSchema.getValueType().getType() == Schema.Type.UNION) {
            Map<String, Object> map = (Map<String, Object>) record;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                TableRow subRecord = new TableRow();
                subRecord.put("key", String.valueOf(entry.getKey()));
                subRecord.put("value", translateUnionTypeObject(entry.getValue(), avroSchema.getValueType()));
                subRecords.add(subRecord);
            }
            result = new AbstractMap.SimpleEntry<>(name, subRecords);
        } else if (avroSchema.getValueType().getType() == Schema.Type.FIXED) {
            Map<String, Object> map = (Map<String, Object>) record;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                TableRow subRecord = new TableRow();
                subRecord.put("key", String.valueOf(entry.getKey()));
                subRecord.put("value", translateFixedTypeObject(entry.getValue(), avroSchema.getValueType()));
                subRecords.add(subRecord);
            }
            result = new AbstractMap.SimpleEntry<>(name, subRecords);
        } else if (avroSchema.getValueType().getType() == Schema.Type.MAP) {
            Map<String, Object> map = (Map<String, Object>) record;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                TableRow subRecord = new TableRow();
                subRecord.put("key", String.valueOf(entry.getKey()));
                subRecord.put("value", translateMapTypeObject(entry.getValue(), avroSchema.getValueType()));
                subRecords.add(subRecord);
            }
            result = new AbstractMap.SimpleEntry<>(name, subRecords);
        } else if (avroSchema.getValueType().getType() == Schema.Type.ENUM) {
            Map<String, Object> map = (Map<String, Object>) record;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                TableRow subRecord = new TableRow();
                subRecord.put("key", String.valueOf(entry.getKey()));
                subRecord.put("value", translateEnumTypeObject(entry.getValue()));
                subRecords.add(subRecord);
            }
            result = new AbstractMap.SimpleEntry<>(name, subRecords);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Map.Entry<String, Object> translateArrayTypeObject(Object record, Schema avroSchema, String name) {
        Map.Entry<String, Object> result;
        if (avroSchema.getElementType().getType() == Schema.Type.ARRAY) {
            throw new IllegalArgumentException("Array of array types are not supported.");
        }

        if (avroSchema.getElementType().getType() == Schema.Type.RECORD) {
            List<TableRow> sRecords = new ArrayList<>();
            for (GenericRecord rec : (GenericArray<GenericRecord>) record) {
                sRecords.add(translateRecord((GenericData.Record) rec));
            }
            result = new AbstractMap.SimpleEntry<>(name, sRecords);
        } else if (avroSchema.getElementType().getType() == Schema.Type.MAP) {
            List<Map<String, Object>> sRecords = new ArrayList<>();
            for (Object rec : (GenericArray<Object>) record) {
                sRecords.addAll((List<TableRow>) translateMapTypeObject(rec, avroSchema.getElementType()));
            }
            result = new AbstractMap.SimpleEntry<>(name, sRecords);
        } else if (avroSchema.getElementType().getType() == Schema.Type.UNION) {
            List<Object> sRecords = new ArrayList<>();
            for (Object rec : (GenericArray<Object>) record) {
                sRecords.add(translateUnionTypeObject(rec, avroSchema.getElementType()));
            }
            result = new AbstractMap.SimpleEntry<>(name, sRecords);
        } else if (avroSchema.getElementType().getType() == Schema.Type.ENUM) {
            List<Object> sRecords = new ArrayList<>();
            for (Object rec : (GenericArray<Object>) record) {
                sRecords.add(translateEnumTypeObject(rec));
            }
            result = new AbstractMap.SimpleEntry<>(name, sRecords);
        } else if (avroSchema.getElementType().getType() == Schema.Type.FIXED) {
            List<Object> sRecords = new ArrayList<>();
            for (Object rec : (GenericArray<Object>) record) {
                sRecords.add(translateFixedTypeObject(rec, avroSchema.getElementType()));
            }
            result = new AbstractMap.SimpleEntry<>(name, sRecords);
        } else {
            List<Object> sRecords = new ArrayList<>();
            for (Object rec : (GenericArray<Object>) record) {
                sRecords.add(translatePrimitiveTypeObject(rec, avroSchema.getElementType()));
            }
            result = new AbstractMap.SimpleEntry<>(name, sRecords);

        }

        return result;
    }

    @SuppressWarnings("unchecked")
    private static TableRow translateRecord(GenericData.Record record) {

        Schema avroSchema = record.getSchema();

        if (avroSchema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException("Object is not a Avro Record type.");
        }

        TableRow fields = new TableRow();
        for (org.apache.avro.Schema.Field avroField: avroSchema.getFields()) {
            if (isPrimitiveType(avroField.schema().getType())) {
                fields.put(avroField.name(),
                        translatePrimitiveTypeObject(record.get(avroField.name()), avroField.schema()));
            } else if (avroField.schema().getType() == Schema.Type.RECORD) {
                if (record.get(avroField.name()) != null) {
                    fields.put(avroField.name(), translateRecord((GenericData.Record) record.get(avroField.name())));
                }
            } else if (avroField.schema().getType() == Schema.Type.UNION) {
                if (record.get(avroField.name()) != null && LogicalTypeIdentifier.isLSN(avroField.name())) {
                    fields.put(avroField.name(),
                            LogicalTypeTranslator.translateLSN(String.valueOf(record.get(avroField.name()))));
                } else {
                    fields.put(avroField.name(), translateUnionTypeObject(record.get(avroField.name()), avroField.schema()));
                }
            } else if (avroField.schema().getType() == Schema.Type.FIXED) {
                fields.put(avroField.name(), translateFixedTypeObject(record.get(avroField.name()), avroField.schema()));
            } else if (avroField.schema().getType() == Schema.Type.MAP) {
                fields.put(avroField.name(), translateMapTypeObject(record.get(avroField.name()), avroField.schema()));
            } else if (avroField.schema().getType() == Schema.Type.ENUM) {
                fields.put(avroField.name(), translateEnumTypeObject(record.get(avroField.name())));
            } else if (avroField.schema().getType() == Schema.Type.ARRAY) {
                fields.put(avroField.name(), translateArrayTypeObject(record.get(avroField.name()), avroField.schema()));
            }
        }
        return fields;
    }

    /**
     * translate given avro record into BQ row object.
     * @param avroRecord avro record
     * @param avroSchema avro schema
     * @return BQ row
     */
    public static InsertAllRequest.RowToInsert translate(GenericRecord avroRecord, Schema avroSchema) {
        if (avroSchema.getType() != org.apache.avro.Schema.Type.RECORD) {
            throw new IllegalArgumentException("The root of the record's schema should be a RECORD type.");
        }
        return InsertAllRequest.RowToInsert.of(translateRecord((GenericData.Record) avroRecord));
    }

}
