package com.linkedin.datastream.kafka;

import java.io.UnsupportedEncodingException;

import com.linkedin.datastream.common.DatastreamRuntimeException;


public class KafkaPassthroughRecordMagicConverter {
  private static final String CHARSET = "ISO-8859-1";

  public static final String PASS_THROUGH_MAGIC_VALUE = "__passThroughMagicValue";

  public static String convertMagicToString(byte magic) {
    try {
      return new String(new byte[] { magic }, CHARSET);
    } catch (UnsupportedEncodingException e) {
      throw new DatastreamRuntimeException(String.format("Cannot convert magic byte %02X to String", magic), e);
    }
  }

  public static byte[] convertMagicStringToByteArray(String magic) {
    try {
      return magic.getBytes(CHARSET);
    } catch (UnsupportedEncodingException e) {
      throw new DatastreamRuntimeException(String.format("Cannot convert magic String %s to byte array", magic),
          e);
    }
  }
}
