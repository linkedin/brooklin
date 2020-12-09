/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common.translator;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

/**
 * Connector that implements LongTranslator and SchemaTranslator to support Json.
 */
public class LongTranslator implements RecordTranslator<Long, GenericRecord>, SchemaTranslator<Long, Schema> {
    /**
     * Translates values of record into the internal format
     *
     * @param record - The record to be translated into the internal format
     * @return The translated record in the internal format
     * @throws Exception if any error occurs during creation
     */
    @Override
    public GenericRecord translateToInternalFormat(Long record) {
        GenericRecord longGenericRecord = new GenericRecordBuilder(this.translateSchemaToInternalFormat(record)).build();
        longGenericRecord.put(TranslatorConstants.AVRO_SCHEMA_KEY, record);
        return longGenericRecord;
    }

    /**
     * Translates values from internal format into T format
     *
     * @param sourceRecord - source schema
     * @return The translated record in T format
     */
    @Override
    public Schema translateSchemaToInternalFormat(Long sourceRecord) {
        return SchemaBuilder.record(TranslatorConstants.AVRO_SCHEMA_RECORD_NAME)
                .fields()
                .optionalLong(TranslatorConstants.AVRO_SCHEMA_KEY)
                .endRecord();
    }
}


