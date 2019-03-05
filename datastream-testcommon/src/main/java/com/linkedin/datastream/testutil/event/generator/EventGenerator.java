/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil.event.generator;

public interface EventGenerator {
  enum EventType {
    INSERT,
    UPDATE,
    DELETE,
    CONTROL
  }

  void generateInsertsOnly(int numEvents);

  void generateUpdatesOnly(int numEvents, long startScn, long endScn);

  void generateDeletesOnly(int numEvents, long startScn, long endScn);
}
