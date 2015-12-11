package com.linkedin.datastream.common;

import com.linkedin.restli.internal.server.util.DataMapUtils;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;


public class DatastreamJSonUtil {
  public static Datastream getDatastreamFromJsonString(String data) {
    InputStream in = IOUtils.toInputStream(data);
    try {
      Datastream datastream = DataMapUtils.read(in, Datastream.class);
      return datastream;
    } catch (IOException ioe) {
      return null;
    }
  }

  public static String getJSonStringFromDatastream(Datastream datastream) {
    byte[] jsonBytes = DataMapUtils.dataTemplateToBytes(datastream, true);
    return new String(jsonBytes, Charset.defaultCharset());
  }
}
