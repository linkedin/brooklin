/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common.translator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * Class to translate between String and internal format
 */
public class StringTranslator implements RecordTranslator<String, GenericRecord>, SchemaTranslator<Schema, String> {
    /**
     * Translates values from internal format into T format
     *
     * @param record        - The record to be translated into the internal format
     * @param includeSchema - Flag to include schema
     * @return The translated record in T format
     * @throws Exception if any error occurs during creation
     */
    @Override
    public String translateFromInternalFormat(GenericRecord record, boolean includeSchema) throws Exception {
        return record.toString();
    }
}
