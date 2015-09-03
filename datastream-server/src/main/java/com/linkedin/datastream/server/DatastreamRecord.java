package com.linkedin.datastream.server;

import java.util.Map;

public interface DatastreamRecord<K, V> {
  // Returns the metadata map associated to the datastream event.
  Map<String, String> getMetadata();

  // Returns the key of the change capture event.
  K getKey();
  // Returns the payload of the change capture event. Typically this will contain the serialized row image.
  V getPayload();

  // Returns the previous payload of the change capture event.  Typically this will contain the serialized row image beforfe
  // the update. This will typically be null for the insert events and boostrap data events.
  V getPreviousPayload();
}
