/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.providers.CheckpointProvider;


/**
 * An in-memory implementation of {@link CheckpointProvider}
 */
public class InMemoryCheckpointProvider implements CheckpointProvider {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryCheckpointProvider.class);

  private final Map<DatastreamTask, Map<Integer, String>> _cpMap = new HashMap<>();

  @Override
  public List<BrooklinMetricInfo> getMetricInfos() {
    return null;
  }

  @Override
  public void unassignDatastreamTask(DatastreamTask task) {
    _cpMap.remove(task);
  }

  @Override
  public void updateCheckpoint(DatastreamTask task, int partition, String checkpoint) {
    if (!_cpMap.containsKey(task)) {
      _cpMap.put(task, new HashMap<>());
    }

    _cpMap.get(task).put(partition, checkpoint);
  }

  @Override
  public void flush() {
  }

  @Override
  public Map<Integer, String> getSafeCheckpoints(DatastreamTask task) {
    return _cpMap.get(task);
  }

  @Override
  public Map<Integer, String> getCommitted(DatastreamTask datastreamTask) {
    if (_cpMap.containsKey(datastreamTask)) {
      return _cpMap.get(datastreamTask);
    } else {
      return new HashMap<>();
    }
  }
}
