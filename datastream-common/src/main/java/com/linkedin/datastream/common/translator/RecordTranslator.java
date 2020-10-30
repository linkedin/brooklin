/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common.translator;

/**
 * Interface to translate data from one format into another format
 * @param <T>
 * @param <I>
 */
public interface RecordTranslator<T, I> {

    /**
     * Translates values of record into the internal format
     *
     * @param record - The record to be translated into the internal format
     * @return The translated record in the internal format
     * @throws Exception if any error occurs during creation
     */
    default I translateToInternalFormat(T record) throws Exception {
        return null;
    }

    /**
     * Translates values from internal format into T format
     *
     * @param record - The record to be translated into the internal format
     * @param includeSchema - Flag to include schema
     * @return The translated record in T format
     * @throws Exception if any error occurs during creation
     */
    default T translateFromInternalFormat(I record, boolean includeSchema) throws Exception {
        return null;
    }
}
