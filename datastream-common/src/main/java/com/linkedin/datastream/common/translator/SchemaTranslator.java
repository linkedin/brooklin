/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common.translator;

/**
 * Interface to translate schema from one format into another format
 * @param <ST>
 * @param <IT>
 */
public interface SchemaTranslator<ST, IT> {
    /**
     * Translates schema into internal format
     *
     * @param sourceSchema - source schema
     * @return The translated record in T format
     */
    default IT translateSchemaToInternalFormat(ST sourceSchema) throws Exception {
        return null;
    }

    /**
     * Translates schema from internal format into ST format
     *
     * @param destinationSchema - source schema
     * @return The translated record in T format
     */
    default ST translateSchemaFromInternalFormat(IT destinationSchema) throws Exception {
        return null;
    }
}
