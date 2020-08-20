/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery.translator;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.LogicalType;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;

/**
 * This class translates given avro schema into BQ schema.
 */
public class SchemaTranslator {

    private static class FieldTypePair {
        Field field;
        StandardSQLTypeName type;

        FieldTypePair(Field field, StandardSQLTypeName type) {
            this.field = field;
            this.type = type;
        }
    }

    private static FieldList translateRecordSchema(org.apache.avro.Schema avroSchema) {
        FieldTypePair subFieldType;
        List<Field> fieldList = new ArrayList<>();
        for (org.apache.avro.Schema.Field avroField: avroSchema.getFields()) {
            if (avroField.schema().getType() == org.apache.avro.Schema.Type.RECORD) {
                fieldList.add(Field.newBuilder(
                        avroField.name(),
                        StandardSQLTypeName.STRUCT,
                        translateRecordSchema(avroField.schema())).setMode(Field.Mode.REQUIRED).build());
            } else {
                subFieldType = translateNonRecordSchema(avroField.schema(), avroField.name());
                if (subFieldType != null) {
                    fieldList.add(subFieldType.field);
                }
            }
        }
        return FieldList.of(fieldList);
    }

    private static FieldTypePair translateNonRecordSchema(org.apache.avro.Schema avroSchema, String name) {

        Field.Builder fieldBuilder;
        StandardSQLTypeName type;
        FieldTypePair fieldTypePair;
        FieldTypePair subFieldType;

        Field.Mode mode = Field.Mode.REQUIRED;

        List<Field> fieldList = new ArrayList<>();

        switch (avroSchema.getType()) {
            case ENUM:
                type = StandardSQLTypeName.STRING;
                fieldBuilder = Field.newBuilder(name, type);
                break;
            case ARRAY:
                if (avroSchema.getElementType().getType() == org.apache.avro.Schema.Type.ARRAY) {
                    throw new IllegalArgumentException("Array of array types are not supported.");
                }

                if (avroSchema.getElementType().getType() == org.apache.avro.Schema.Type.RECORD) {
                    type = StandardSQLTypeName.STRUCT;
                    fieldBuilder = Field.newBuilder(name,
                            StandardSQLTypeName.STRUCT,
                            translateRecordSchema(avroSchema.getElementType()));
                } else {
                    fieldTypePair = translateNonRecordSchema(avroSchema.getElementType(), name);
                    if (fieldTypePair == null) {
                        return null;
                    }
                    type = fieldTypePair.type;

                    if (type == StandardSQLTypeName.STRUCT) {
                        fieldBuilder = Field.newBuilder(name, type, fieldTypePair.field.getSubFields());
                    } else {
                        fieldBuilder = Field.newBuilder(name, type);
                    }
                }
                mode = Field.Mode.REPEATED;
                break;
            case MAP:
                type = StandardSQLTypeName.STRUCT;
                if (avroSchema.getValueType().getType() == org.apache.avro.Schema.Type.RECORD) {
                    fieldList = FieldList.of(
                            Field.newBuilder("key", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                            Field.newBuilder("value", StandardSQLTypeName.STRUCT,
                                    translateRecordSchema(avroSchema.getValueType())).setMode(Field.Mode.REQUIRED).build()
                    );
                } else {
                    subFieldType = translateNonRecordSchema(avroSchema.getValueType(), name);
                    if (subFieldType == null) {
                        return null;
                    }

                    if (subFieldType.type == StandardSQLTypeName.STRUCT) {
                        if (avroSchema.getValueType().getType() == org.apache.avro.Schema.Type.ARRAY ||
                                avroSchema.getValueType().getType() == org.apache.avro.Schema.Type.MAP) {
                            fieldList = FieldList.of(
                                    Field.newBuilder("key", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                                    Field.newBuilder("value", subFieldType.type, subFieldType.field.getSubFields()).setMode(Field.Mode.REPEATED).build()

                            );
                        } else {
                            fieldList = FieldList.of(
                                    Field.newBuilder("key", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                                    Field.newBuilder("value", subFieldType.type, subFieldType.field.getSubFields()).setMode(Field.Mode.REQUIRED).build()

                            );
                        }
                    } else {
                        if (avroSchema.getValueType().getType() == org.apache.avro.Schema.Type.ARRAY) {
                            fieldList = FieldList.of(
                                    Field.newBuilder("key", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                                    Field.newBuilder("value", subFieldType.type).setMode(Field.Mode.REPEATED).build()
                            );
                        } else {
                            fieldList = FieldList.of(
                                    Field.newBuilder("key", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                                    Field.newBuilder("value", subFieldType.type).setMode(Field.Mode.REQUIRED).build()
                            );
                        }
                    }
                }
                fieldBuilder = Field.newBuilder(name, type, FieldList.of(fieldList));
                mode = Field.Mode.REPEATED;
                break;
            case UNION:
                if (avroSchema.getTypes().size() == 2 &&
                        (avroSchema.getTypes().get(0).getType() == org.apache.avro.Schema.Type.NULL ||
                                avroSchema.getTypes().get(1).getType() == org.apache.avro.Schema.Type.NULL)
                ) {
                    org.apache.avro.Schema childSchema = (avroSchema.getTypes().get(0).getType() != org.apache.avro.Schema.Type.NULL) ?
                            avroSchema.getTypes().get(0) : avroSchema.getTypes().get(1);

                    if (childSchema.getType() == org.apache.avro.Schema.Type.RECORD) {
                        type = StandardSQLTypeName.STRUCT;
                        fieldBuilder = Field.newBuilder(name,
                                StandardSQLTypeName.STRUCT,
                                translateRecordSchema(childSchema));
                        mode = (childSchema.getType() == org.apache.avro.Schema.Type.ARRAY) ? Field.Mode.REPEATED : Field.Mode.NULLABLE;
                    } else {
                        fieldTypePair = translateNonRecordSchema(childSchema, name);
                        if (fieldTypePair == null) {
                            return null;
                        }

                        type = fieldTypePair.type;
                        if (type == StandardSQLTypeName.STRUCT) {
                            fieldBuilder = Field.newBuilder(name, fieldTypePair.type, fieldTypePair.field.getSubFields());
                            mode = Field.Mode.REPEATED;
                        } else {
                            fieldBuilder = Field.newBuilder(name, fieldTypePair.type);
                            mode = (childSchema.getType() == org.apache.avro.Schema.Type.ARRAY) ? Field.Mode.REPEATED : Field.Mode.NULLABLE;
                        }
                    }

                } else {
                    mode = Field.Mode.REQUIRED;
                    for (org.apache.avro.Schema uType : avroSchema.getTypes()) {
                        if (uType.getType() == org.apache.avro.Schema.Type.NULL) {
                            mode = Field.Mode.NULLABLE;
                        }

                        if (uType.getType() == org.apache.avro.Schema.Type.RECORD) {
                            Field.Builder fb = Field.newBuilder(uType.getName().toLowerCase() + "_value",
                                    StandardSQLTypeName.STRUCT, translateRecordSchema(uType));
                            fb.setMode(Field.Mode.NULLABLE);
                            fieldList.add(fb.build());
                        } else if (uType.getType() == org.apache.avro.Schema.Type.ENUM) {
                            Field.Builder fb = Field.newBuilder(uType.getName().toLowerCase() + "_value", StandardSQLTypeName.STRING);
                            fb.setMode(Field.Mode.NULLABLE);
                            fieldList.add(fb.build());
                        } else if (uType.getType() == org.apache.avro.Schema.Type.FIXED) {
                            if (LogicalTypeIdentifier.isDecimalType(uType)) {
                                Field.Builder fb = Field.newBuilder(uType.getName().toLowerCase()
                                                + "_"
                                                + uType.getLogicalType().getName().toLowerCase()
                                                + "_value",
                                        StandardSQLTypeName.NUMERIC);
                                fb.setMode(Field.Mode.NULLABLE);
                                fieldList.add(fb.build());
                            } else {
                                Field.Builder fb = Field.newBuilder(uType.getName().toLowerCase() + "_value", StandardSQLTypeName.BYTES);
                                fb.setMode(Field.Mode.NULLABLE);
                                fieldList.add(fb.build());
                            }
                        } else {
                            subFieldType = translateNonRecordSchema(uType, name);
                            if (subFieldType == null) {
                                continue;
                            }

                            Field.Builder fb;
                            if (uType.getType() == org.apache.avro.Schema.Type.MAP) {
                                LogicalType lType = uType.getValueType().getLogicalType();
                                if (lType == null) {
                                    fb = Field.newBuilder("map_" + uType.getValueType().getType().getName().toLowerCase() + "_value",
                                            subFieldType.type,
                                            subFieldType.field.getSubFields());
                                } else {
                                    fb = Field.newBuilder("map_" +
                                                    uType.getValueType().getType().getName().toLowerCase() +
                                                    "_" +
                                                    lType.getName().toLowerCase().replace("-", "_") +
                                                    "_value",
                                            subFieldType.type,
                                            subFieldType.field.getSubFields());
                                }
                                fb.setMode(Field.Mode.REPEATED);
                            } else if (uType.getType() == org.apache.avro.Schema.Type.ARRAY) {
                                if (subFieldType.type == StandardSQLTypeName.STRUCT) {
                                    fb = Field.newBuilder("array_" + uType.getElementType().getType().getName().toLowerCase() + "_value",
                                            subFieldType.type,
                                            subFieldType.field.getSubFields());
                                } else {
                                    LogicalType lType = uType.getElementType().getLogicalType();
                                    if (lType == null) {
                                        fb = Field.newBuilder("array_" + uType.getElementType().getType().getName().toLowerCase() + "_value",
                                                subFieldType.type);
                                    } else {
                                        fb = Field.newBuilder("array_" +
                                                        uType.getElementType().getType().getName().toLowerCase() +
                                                        "_" +
                                                        lType.getName().toLowerCase().replace("-", "_") +
                                                        "_value",
                                                subFieldType.type);
                                    }
                                }
                                fb.setMode(Field.Mode.REPEATED);
                            } else {
                                if (uType.getLogicalType() == null || uType.getLogicalType().getName().equals("uuid")) {
                                    fb = Field.newBuilder(uType.getType().getName().toLowerCase() + "_value", subFieldType.type);
                                } else {
                                    fb = Field.newBuilder(uType.getType().getName().toLowerCase() +
                                                    "_" +
                                                    uType.getLogicalType().getName().toLowerCase().replace("-", "_") +
                                                    "_value",
                                            subFieldType.type,
                                            subFieldType.field.getSubFields());
                                }
                                fb.setMode(Field.Mode.NULLABLE);
                            }
                            fieldList.add(fb.build());
                        }
                    }
                    type = StandardSQLTypeName.STRUCT;
                    fieldBuilder = Field.newBuilder(name, type, FieldList.of(fieldList));
                }
                break;
            case FIXED:
            case BYTES:
                if (LogicalTypeIdentifier.isDecimalType(avroSchema)) {
                    type = StandardSQLTypeName.NUMERIC;
                    fieldBuilder = Field.newBuilder(name, type);
                    break;
                }
                // covers logical type Duration as well
                type = StandardSQLTypeName.BYTES;
                fieldBuilder = Field.newBuilder(name, type);
                break;
            case BOOLEAN:
                type = StandardSQLTypeName.BOOL;
                fieldBuilder = Field.newBuilder(name, type);
                break;
            case INT:
                if (LogicalTypeIdentifier.isDateType(avroSchema)) {
                    type = StandardSQLTypeName.DATE;
                    fieldBuilder = Field.newBuilder(name, type);
                    break;
                }
            case LONG:
                if (LogicalTypeIdentifier.isTimeType(avroSchema)) {
                    type = StandardSQLTypeName.TIME;
                    fieldBuilder = Field.newBuilder(name, type);
                } else if (LogicalTypeIdentifier.isTimestampType(avroSchema)) {
                    type = StandardSQLTypeName.TIMESTAMP;
                    fieldBuilder = Field.newBuilder(name, type);
                } else if (LogicalTypeIdentifier.isDatetimeType(avroSchema)) {
                    type = StandardSQLTypeName.DATETIME;
                    fieldBuilder = Field.newBuilder(name, type);
                } else {
                    type = StandardSQLTypeName.INT64;
                    fieldBuilder = Field.newBuilder(name, type);
                }
                break;
            case FLOAT:
            case DOUBLE:
                type = StandardSQLTypeName.FLOAT64;
                fieldBuilder = Field.newBuilder(name, type);
                break;
            case STRING:
                if (LogicalTypeIdentifier.isZonedTime(avroSchema)) {
                    type = StandardSQLTypeName.TIME;
                    fieldBuilder = Field.newBuilder(name, type);
                } else if (LogicalTypeIdentifier.isZonedTimestamp(avroSchema)) {
                    type = StandardSQLTypeName.TIMESTAMP;
                    fieldBuilder = Field.newBuilder(name, type);
                } else if (LogicalTypeIdentifier.isLSN(name)) {
                    type = StandardSQLTypeName.NUMERIC;
                    fieldBuilder = Field.newBuilder(name, type);
                } else {
                    type = StandardSQLTypeName.STRING;
                    fieldBuilder = Field.newBuilder(name, type);
                }
                break;
            case NULL:
                return null;
            default:
                throw new IllegalArgumentException("Avro type not recognized.");
        }

        if (avroSchema.getDoc() != null) {
            fieldBuilder.setDescription(avroSchema.getDoc());
        }

        return new FieldTypePair(fieldBuilder.setMode(mode).build(), type);
    }

    /**
     * Translate given avro schema into BQ schema
     * @param avroSchema avro schema
     * @return BQ schema
     */
    public static Schema translate(org.apache.avro.Schema avroSchema) {
        if (avroSchema.getType() != org.apache.avro.Schema.Type.RECORD) {
            throw new IllegalArgumentException("The root of the record's schema should be a RECORD type.");
        }
        return Schema.of(translateRecordSchema(avroSchema));
    }
}
