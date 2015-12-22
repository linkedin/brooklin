package com.linkedin.datastream.testutil.event.generator;

public interface EventGenerator {
  public enum EVENT_TYPE {
    INSERT,
    UPDATE,
    DELETE,
    CONTROL
  }

  void generateInsertsOnly(int numEvents);

  void generateUpdatesOnly(int numEvents, long startScn, long EndScn);

  void generateDeletesOnly(int numEvents, long startScn, long EndScn);

}
