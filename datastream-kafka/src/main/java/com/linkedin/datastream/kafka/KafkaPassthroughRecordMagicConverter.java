/**
 *  Copyright 2020 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.kafka;

import java.io.UnsupportedEncodingException;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import com.linkedin.datastream.common.DatastreamRuntimeException;


/**
 * This class is a utility class to convert the PassthroughConsumerRecord's magic() to a String, and back into byte.
 * It also adds a utility to create the RecordHeaders which can be used to pass headers with the magic set to the
 * ProducerRecord for passthrough message format bump support.
 */
public class KafkaPassthroughRecordMagicConverter {
  private static final String CHARSET = "ISO-8859-1";

  public static final String PASS_THROUGH_MAGIC_VALUE = "__passThroughMagicValue";

  /**
   * Convert the magic byte of the PassthroughConsumerRecord to a String
   */
  public static String convertMagicToString(byte magic) {
    try {
      return new String(new byte[] { magic }, CHARSET);
    } catch (UnsupportedEncodingException e) {
      throw new DatastreamRuntimeException(String.format("Cannot convert magic byte %02X to String", magic), e);
    }
  }

  /**
   * Convert the converted magic String back into a byte array
   */
  public static byte[] convertMagicStringToByteArray(String magic) {
    try {
      return magic.getBytes(CHARSET);
    } catch (UnsupportedEncodingException e) {
      throw new DatastreamRuntimeException(String.format("Cannot convert magic String %s to byte array", magic),
          e);
    }
  }

  /**
   * Return a RecordHeaders containing the correct Passthrough magic header which can be used to create the
   * ProducerRecord to send the passthrough record
   */
  public static RecordHeaders convertMagicStringToRecordHeaders(String magicString) {
    RecordHeaders recordHeaders = null;
    if (magicString != null) {
      byte[] magic = convertMagicStringToByteArray(magicString);
      RecordHeader recordHeader = new RecordHeader(KafkaPassthroughRecordMagicConverter.PASS_THROUGH_MAGIC_VALUE,
          magic);
      recordHeaders = new RecordHeaders();
      recordHeaders.add(recordHeader);
    }
    return recordHeaders;
  }
}
