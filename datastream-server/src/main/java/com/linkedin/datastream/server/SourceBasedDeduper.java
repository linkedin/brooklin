/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;


/**
 * Deduper that uses the source to figure out whether two datastreams can be de-duped.
 */
public class SourceBasedDeduper extends AbstractDatastreamDeduper {
  private static final Logger LOG = LoggerFactory.getLogger(SourceBasedDeduper.class);

  @Override
  public Optional<Datastream> dedupStreams(Datastream stream, List<Datastream> candidates)
      throws DatastreamValidationException {
    List<Datastream> duplicateDatastreams = candidates.stream()
        .filter(d -> d.getSource().equals(stream.getSource()))
        .collect(Collectors.toList());

    Optional<Datastream> reusedStream = Optional.empty();

    if (!duplicateDatastreams.isEmpty()) {
      reusedStream = Optional.of(duplicateDatastreams.get(0));
      LOG.info("Found duplicate datastreams {} for datastream {}, picked {}",
          duplicateDatastreams, stream, reusedStream);
    }

    return reusedStream;
  }
}
