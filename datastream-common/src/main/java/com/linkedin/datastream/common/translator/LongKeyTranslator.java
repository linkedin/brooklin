/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common.translator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * Class to translate Avro Key record to Long
 */
public class LongKeyTranslator implements RecordTranslator<Long, GenericRecord>, SchemaTranslator<Schema, Schema> {

    /**
     * Extract Key from the internal record
     *
     * @param record        - The record to be translated into the new format
     * @param includeSchema - Flag to include schema
     * @return The translated record in
     * @throws Exception if any error occurs during creation
     */
    @Override
    public Long translateFromInternalFormat(GenericRecord record, boolean includeSchema) throws Exception {
        return (Long) record.get(TranslatorConstants.AVRO_SCHEMA_KEY);
    }
}
