/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.cloud.storage;

/**
 * Record class, key-value pair
 */
public class Record {

    private Object _key;
    private Object _value;

    /**
     * Constructor for Record class
     * @param key key of the record
     * @param value value of the record
     */
    public Record(Object key, Object value) {
        this._key = key;
        this._value = value;
    }

    /**
     * returns the key of the record
     */
    public Object getKey() {
        return _key;
    }

    /**
     * returns the value of the record
     */
    public Object getValue() {
        return _value;
    }
}

