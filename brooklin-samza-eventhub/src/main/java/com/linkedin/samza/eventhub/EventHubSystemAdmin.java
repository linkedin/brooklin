package com.linkedin.samza.eventhub;

import java.util.Map;
import java.util.Set;

import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;


public class EventHubSystemAdmin implements SystemAdmin {
  @Override
  public Map<SystemStreamPartition, String> getOffsetsAfter(Map<SystemStreamPartition, String> offsets) {
    return null;
  }

  @Override
  public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
    return null;
  }

  @Override
  public void createChangelogStream(String streamName, int numOfPartitions) {
  }

  @Override
  public void validateChangelogStream(String streamName, int numOfPartitions) {

  }

  @Override
  public void createCoordinatorStream(String streamName) {

  }

  @Override
  public Integer offsetComparator(String offset1, String offset2) {
    return null;
  }
}
