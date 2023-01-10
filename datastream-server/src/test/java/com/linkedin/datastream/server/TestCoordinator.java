package com.linkedin.datastream.server;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.testutil.DatastreamTestUtils;


public class TestCoordinator {
  @Test
  public void testInferStoppingStreamsFromAssignment() {
    String connectorType = "testConnector";
    Datastream ketchupStream = DatastreamTestUtils.createDatastream(connectorType, "ketchupStream", "random");
    Datastream mayoStream = DatastreamTestUtils.createDatastream(connectorType, "mayoStream", "random");
    Datastream mustardStream = DatastreamTestUtils.createDatastream(connectorType, "mustardStream", "random");

    DatastreamTaskImpl task1 = new DatastreamTaskImpl();
    task1.setTaskPrefix(ketchupStream.getName());
    task1.setDatastreams(Collections.singletonList(ketchupStream));
    DatastreamTaskImpl task2 = new DatastreamTaskImpl();
    task2.setTaskPrefix(mustardStream.getName());
    task2.setDatastreams(Collections.singletonList(mustardStream));
    List<DatastreamTask> newAssignment = Arrays.asList(task1, task2);

    DatastreamTaskImpl task3 = new DatastreamTaskImpl();
    task3.setTaskPrefix(mayoStream.getName());
    task3.setDatastreams(Collections.singletonList(mayoStream));
    List<DatastreamTask> removedTasks = Collections.singletonList(task3);

    List<Datastream> stoppingStreams = Coordinator.inferStoppingDatastreamsFromAssignment(newAssignment, removedTasks);
    Assert.assertEquals(stoppingStreams.size(), 1);
    Assert.assertEquals(stoppingStreams.get(0), mayoStream);

    removedTasks = Arrays.asList(task2, task3);
    stoppingStreams = Coordinator.inferStoppingDatastreamsFromAssignment(newAssignment, removedTasks);
    Assert.assertEquals(stoppingStreams.size(), 1);
    Assert.assertEquals(stoppingStreams.get(0), mayoStream);

    newAssignment = Arrays.asList(task1, task2, task3);
    stoppingStreams = Coordinator.inferStoppingDatastreamsFromAssignment(newAssignment, removedTasks);
    Assert.assertEquals(stoppingStreams.size(), 0);
  }

}
