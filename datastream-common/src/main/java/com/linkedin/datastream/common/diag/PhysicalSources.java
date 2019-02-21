/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common.diag;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;


/**
 * This object holds a mapping between a physical source (by its name) to its position.
 * @see PhysicalSourcePosition
 */
public class PhysicalSources {

  /**
   * A mapping from a physical source's name to its position.
   */
  private final Map<String, PhysicalSourcePosition> _physicalSourceToPosition = new ConcurrentHashMap<>();

  /**
   * Default constructor.
   */
  public PhysicalSources() {
  }

  /**
   * Retains only the physical sources in the specified collection (removes all others). In other words, removes from
   * the mappings of physical source -> position, all entries where the physical source is not contained in the
   * specified collection.
   * @param physicalSources the list of physical sources to keep
   */
  public void retainAll(final Collection<String> physicalSources) {
    _physicalSourceToPosition.keySet().retainAll(physicalSources);
  }

  /**
   * Gets the physical source's existing position.
   * @param physicalSource the physical source name
   * @return the physical source's current position data
   */
  public PhysicalSourcePosition get(final String physicalSource) {
    return _physicalSourceToPosition.get(physicalSource);
  }

  /**
   * Sets the physical source's existing position.
   * @param physicalSource the physical source name
   * @param physicalSourcePosition the physical source's current position data
   */
  public void set(final String physicalSource, final PhysicalSourcePosition physicalSourcePosition) {
    _physicalSourceToPosition.put(physicalSource, physicalSourcePosition);
  }

  /**
   * Getter returns the current physical source to position map.
   *
   * This method is used for JSON serialization.
   *
   * @return an immutable copy of the current physical source to position map
   */
  public Map<String, PhysicalSourcePosition> getPhysicalSourceToPosition() {
    return Collections.unmodifiableMap(_physicalSourceToPosition);
  }

  /**
   * Setter causes the entry set of the current physical source to position map to match the provided one. If a null
   * map is provided, the current map will be cleared.
   *
   * This method is used for JSON serialization.
   *
   * @param physicalSourceToPosition a map to set the physical sources to position map to
   */
  public void setPhysicalSourceToPosition(final Map<String, PhysicalSourcePosition> physicalSourceToPosition) {
    _physicalSourceToPosition.clear();
    if (physicalSourceToPosition != null) {
      _physicalSourceToPosition.putAll(physicalSourceToPosition);
    }
  }

  /**
   * Merges two PhysicalSources objects together using the freshest data available between the position data
   * in the PhysicalSourcePosition for each physical source.
   * @param first the first PhysicalSources object
   * @param second the second PhysicalSources object
   * @return a merged PhysicalSources object
   */
  public static PhysicalSources merge(final PhysicalSources first, final PhysicalSources second) {
    final PhysicalSources newSources = new PhysicalSources();
    newSources.setPhysicalSourceToPosition(first._physicalSourceToPosition);
    if (second != null) {
      second._physicalSourceToPosition.forEach((source, secondPosition) -> {
        final PhysicalSourcePosition firstPosition = Optional.ofNullable(first.get(source))
            .orElseGet(PhysicalSourcePosition::new);
        final PhysicalSourcePosition newestPosition = Stream.of(firstPosition, secondPosition)
            .max(Comparator.comparingLong(p -> Long.max(p.getConsumerProcessedTimeMs(), p.getSourceQueriedTimeMs())))
            .get();
        newSources._physicalSourceToPosition.put(source, newestPosition);
      });
    }
    return newSources;
  }

  /**
   * A simple String representation of this object suitable for use in debugging or logging.
   * @return a simple String representation of this object
   */
  @Override
  public String toString() {
    return "PhysicalSources{" + "_physicalSourceToPosition=" + _physicalSourceToPosition + '}';
  }
}