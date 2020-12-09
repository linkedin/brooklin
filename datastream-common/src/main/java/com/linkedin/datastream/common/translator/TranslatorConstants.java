/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common.translator;

/**
 * Various well known config keys used in translators
 */
public class TranslatorConstants {

    /**
     * Represents the key in the Avro record
     */
    protected static final String AVRO_SCHEMA_KEY = "key";

    /**
     *  Represents the record name in the Avro schema
     */
    protected static final String AVRO_SCHEMA_RECORD_NAME = "sqlRecord";
}
