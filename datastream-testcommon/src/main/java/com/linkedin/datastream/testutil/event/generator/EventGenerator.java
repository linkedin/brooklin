/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil.event.generator;

/**
 * An abstraction of the APIs needed for a module that generates (insert/update/delete) events
 */
public interface EventGenerator {
  /**
   * Types of events to generate
   */
  enum EventType {
    INSERT,
    UPDATE,
    DELETE,
    CONTROL
  }

  /**
   * Generate as many insert events as {@code numEvents} only
   */
  void generateInsertsOnly(int numEvents);

  /**
   * Generate as many update events as {@code numEvents} only,
   * starting/ending with the provided start and end SCNs, respectively
   */
  void generateUpdatesOnly(int numEvents, long startScn, long endScn);

  /**
   * Generate as many delete events as {@code numEvents} only,
   * starting/ending with the provided start and end SCNs, respectively
   */
  void generateDeletesOnly(int numEvents, long startScn, long endScn);
}
