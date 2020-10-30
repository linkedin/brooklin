/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common.translator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to translate between GenericRecord and internal format
 */
public class GenericRecordTranslator implements RecordTranslator<GenericRecord, GenericRecord>, SchemaTranslator<Schema, Schema> {

    private static final Logger LOG = LoggerFactory.getLogger(GenericRecordTranslator.class.getSimpleName());
    /**
     * Translates values of record into the internal format
     *
     * @param record - The record to be translated into the internal format
     * @return The translated record in the internal format
     * @throws Exception if any error occurs during creation
     */
    @Override
    public GenericRecord translateToInternalFormat(GenericRecord record) throws Exception {
        return record;
    }

    /**
     * Translates values from internal format into T format
     *
     * @param record - The record to be translated into the internal format
     * @param includeSchema - Flag to include schema
     * @return The translated record in T format
     * @throws Exception if any error occurs during creation
     */
    @Override
    public GenericRecord translateFromInternalFormat(GenericRecord record, boolean includeSchema) throws Exception {
        return record;
    }
}
