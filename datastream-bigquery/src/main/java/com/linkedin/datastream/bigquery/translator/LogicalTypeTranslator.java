/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery.translator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.TimeZone;

import org.apache.avro.Schema;

import com.google.api.client.util.DateTime;

/**
 * This class has methods to translate avro logical types to appropriate BQ type.
 */
class LogicalTypeTranslator {

    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final String TIME_FORMAT = "HH:mm:ss";
    private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

    /**
     * Translates avro date to BQ date
     * @param daysSinceEpoch days since epoch
     * @return BQ date
     */
    static String translateDateType(long daysSinceEpoch) {
        return new SimpleDateFormat(DATE_FORMAT).format(new Date(daysSinceEpoch * 86400000L));
    }

    /**
     * Translates avro timestamp to BQ timestamp
     * @param instant timestamp instant
     * @param avroSchema avro schema
     * @return BQ timestamp
     */
    static DateTime translateTimestampType(long instant, Schema avroSchema) {
        if (LogicalTypeIdentifier.isMilliTimestamp(avroSchema)) {
            return new DateTime(new Date(instant));
        } else if (LogicalTypeIdentifier.isMicroTimestamp(avroSchema)) {
            return new DateTime(new Date(instant / 1000));
        }
        return null;
    }

    /**
     * Translates avro timestamp to BQ datetime
     * @param instant datetime instant
     * @param avroSchema avro schema
     * @return BQ datetime
     */
    static String translateDatetimeType(long instant, Schema avroSchema) {
        SimpleDateFormat dateTimeFmt = new SimpleDateFormat(DATETIME_FORMAT);
        dateTimeFmt.setTimeZone(TimeZone.getTimeZone("UTC"));

        if (LogicalTypeIdentifier.isMilliDatetime(avroSchema)) {
            return dateTimeFmt.format(new Date(instant)) + "000";
        } else if (LogicalTypeIdentifier.isMicroDatetime(avroSchema)) {
            return dateTimeFmt.format(new Date(instant / 1000)) + "000";
        } else if (LogicalTypeIdentifier.isNanoDatetime(avroSchema)) {
            return dateTimeFmt.format(new Date(instant / 1000000)) + "000";
        }
        return null;
    }

    /**
     * Translates avro zoned timestamp to BQ timestamp
     * @param instant timestamp instant
     * @param avroSchema avro schema
     * @return BQ timestamp
     */
    static DateTime translateTimestampType(String instant, Schema avroSchema) {
        if (LogicalTypeIdentifier.isZonedTimestamp(avroSchema)) {
            return new DateTime(Date.from(OffsetDateTime.parse(instant, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant()));
        }
        return null;
    }

    /**
     * Translates avro time to BQ timestamp
     * @param instant time instant
     * @param avroSchema avro schema
     * @return BQ time
     */
    static String translateTimeType(long instant, Schema avroSchema) {
        SimpleDateFormat timeFmt = new SimpleDateFormat(TIME_FORMAT);
        timeFmt.setTimeZone(TimeZone.getTimeZone("UTC"));
        if (LogicalTypeIdentifier.isMilliTime(avroSchema)) {
            return timeFmt.format(new Date(instant)) + "." + String.format("%06d", (instant % 1000) * 1000);
        } else if (LogicalTypeIdentifier.isMicroTime(avroSchema)) {
            return timeFmt.format(new Date(instant / 1000)) + "." + String.format("%06d", instant % 1000000);
        } else if (LogicalTypeIdentifier.isNanoTime(avroSchema)) {
            return timeFmt.format(new Date(instant / 1000000)) + "." + String.format("%06d", (instant % 1000000000) / 1000);
        }
        return null;
    }

    /**
     * Translates avro zoned time to BQ timestamp
     * @param instant time instant
     * @param avroSchema avro schema
     * @return BQ time
     */
    static String translateTimeType(String instant, Schema avroSchema) {
        SimpleDateFormat timeFmt = new SimpleDateFormat(TIME_FORMAT);
        timeFmt.setTimeZone(TimeZone.getTimeZone("UTC"));
        if (LogicalTypeIdentifier.isZonedTime(avroSchema)) {
            return timeFmt.format(Date.from(OffsetDateTime.parse(instant, DateTimeFormatter.ISO_OFFSET_TIME).toInstant()));
        }
        return null;
    }

    /**
     * Translates hex format LSN to decimal
     * @param lsn LSN in hex string format
     * @return decimal LSN in decimal
     */
    static BigDecimal translateLSN(String lsn) {
        return new BigDecimal(new BigInteger(lsn.replace(":", ""), 16));
    }
}
