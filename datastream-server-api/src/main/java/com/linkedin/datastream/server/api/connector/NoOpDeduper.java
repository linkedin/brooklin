package com.linkedin.datastream.server.api.connector;

import java.util.List;
import java.util.Optional;

import com.linkedin.datastream.common.Datastream;


/**
 * Dummy deduper for connector not needing de-dup or handles de-dups themselves.
 */
public class NoOpDeduper implements DatastreamDeduper {
  @Override
  public Optional<Datastream> findExistingDatastream(Datastream newDatastream, List<Datastream> allDatastream) {
    return Optional.empty();
  }
}
