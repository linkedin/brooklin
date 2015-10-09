package com.linkedin.datastream.server;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.VerifiableProperties;


/**
 * Dummy implementation of Datastream event collector.
 */
public class DummyDatastreamEventCollector implements DatastreamEventCollector {
  public DummyDatastreamEventCollector(Datastream datastream, VerifiableProperties config) {
  }

  @Override
  public void send(DatastreamEventRecord record) {
  }
}
