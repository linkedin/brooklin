package com.linkedin.datastream.common;

import java.util.HashMap;
import java.util.Map;

/**
 * Simple utility class for working with DatastreamEvent objects.
 */
public final class DatastreamEventUtils {
  private DatastreamEventUtils() {
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
  public static void processMetadata(DatastreamEvent event) {
    if (event == null)
      return;
    Map<CharSequence, CharSequence> orig = event.metadata;
    Map<CharSequence, CharSequence> out = new HashMap<>();
    event.metadata.forEach((k, v) -> {
      out.put(new String(k.toString()), new String(v.toString()));
    });

    event.metadata = out;
  }
}
