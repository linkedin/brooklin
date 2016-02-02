package com.linkedin.datastream.common;

import com.linkedin.restli.internal.server.util.DataMapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.Validate;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;


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
    Validate.notNull(datastream.getConnectorType(), "invalid datastream connector type");
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
   * There method is added to workaround a quirk of Avro, where the
   * key type used in a map field is {@link org.apache.avro.util.Utf8}
   * instead of plain Java String. This causes map lookup to fail since
   * we always use plain String as keys.
   *
   * This method recreates DatastreamEvent.metadata with the same
   * entries but converting key/value from Utf8 into String.
   *
   * @param event an Avro serialized DatastreamEvent
   */
  public static void processEventMetadata(DatastreamEvent event) {
    if (event == null) {
      return;
    }
    Map<CharSequence, CharSequence> out = new HashMap<>();
    event.metadata.forEach((k, v) -> {
      out.put(new String(k.toString()), new String(v.toString()));
    });

    event.metadata = out;
  }

  public static DatastreamEvent createEvent() {
    Random random = new Random();
    byte[] bytes = new byte[100];
    random.nextBytes(bytes);
    return createEvent(bytes);
  }

  public static DatastreamEvent createEvent(byte[] payload) {
    DatastreamEvent event = new DatastreamEvent();
    event.payload = ByteBuffer.wrap(payload);
    event.key = ByteBuffer.allocate(0);
    event.metadata = new HashMap<>();
    event.previous_payload = ByteBuffer.allocate(0);
    return event;
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
}
