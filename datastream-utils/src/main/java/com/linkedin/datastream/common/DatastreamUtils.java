package com.linkedin.datastream.common;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.Validate;

import com.linkedin.restli.internal.server.util.DataMapUtils;


/**
 * Simple utility class for working with Datastream-related objects.
 */
public final class DatastreamUtils {
  private DatastreamUtils() {
  }

  /**
   * Validate the validity of fields of a newly created Datastream before received
   * and processed by the Coordinator.
   * @param datastream datastream object to be validated
   */
  public static void validateNewDatastream(Datastream datastream) {
    Validate.notNull(datastream, "invalid datastream");
    Validate.notNull(datastream.getSource(), "invalid datastream source");
    Validate.notNull(datastream.getName(), "invalid datastream name");
    Validate.notNull(datastream.getConnectorName(), "invalid datastream connector type");
  }

  /**
   * Validate the validity of fields of an existing Datastream.
   * @param datastream datastream object to be validated
   */
  public static void validateExistingDatastream(Datastream datastream) {
    validateNewDatastream(datastream);
    Validate.notNull(datastream.getDestination(), "invalid datastream destination");
    Validate.notNull(datastream.getDestination().getConnectionString(), "invalid destination connection");
    Validate.notNull(datastream.getDestination().getPartitions(), "invalid destination partitions");
    if (datastream.getDestination().getPartitions() <= 0) {
      throw new IllegalArgumentException("invalid destination partition count.");
    }
  }

  /**
   * Deserialize JSON text into a Datastream object.
   * @param json
   * @return
   */
  public static Datastream fromJSON(String json) {
    InputStream in = IOUtils.toInputStream(json);
    try {
      Datastream datastream = DataMapUtils.read(in, Datastream.class);
      return datastream;
    } catch (IOException ioe) {
      return null;
    }
  }

  /**
   * Serialize a Datastream object into a JSON text.
   * @param datastream
   * @return
   */
  public static String toJSON(Datastream datastream) {
    byte[] jsonBytes = DataMapUtils.dataTemplateToBytes(datastream, true);
    return new String(jsonBytes, Charset.defaultCharset());
  }

  public static String getTaskPrefix(Datastream datastream) {
    return datastream.getMetadata().get(DatastreamMetadataConstants.TASK_PREFIX);
  }
}
