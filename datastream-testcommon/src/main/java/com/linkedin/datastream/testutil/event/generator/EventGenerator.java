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
