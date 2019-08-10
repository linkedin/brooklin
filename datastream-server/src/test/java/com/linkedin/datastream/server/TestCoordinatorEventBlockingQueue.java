/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import org.testng.Assert;
import org.testng.annotations.Test;


public class TestCoordinatorEventBlockingQueue {

  @Test
  public void testHappyPath() throws Exception{
    CoordinatorEventBlockingQueue eventBlockingQueue = new CoordinatorEventBlockingQueue();
    eventBlockingQueue.put(CoordinatorEvent.LEADER_DO_ASSIGNMENT_EVENT);
    eventBlockingQueue.put(CoordinatorEvent.LEADER_DO_ASSIGNMENT_EVENT);
    eventBlockingQueue.put(CoordinatorEvent.LEADER_DO_ASSIGNMENT_EVENT);
    eventBlockingQueue.put(CoordinatorEvent.HANDLE_ASSIGNMENT_CHANGE_EVENT);
    eventBlockingQueue.put(CoordinatorEvent.HANDLE_ASSIGNMENT_CHANGE_EVENT);
    eventBlockingQueue.put(CoordinatorEvent.HANDLE_ASSIGNMENT_CHANGE_EVENT);
    Assert.assertEquals(eventBlockingQueue.take(), CoordinatorEvent.LEADER_DO_ASSIGNMENT_EVENT);
    Assert.assertEquals(eventBlockingQueue.take(), CoordinatorEvent.HANDLE_ASSIGNMENT_CHANGE_EVENT);
  }


  @Test
  public void testEventWithMetadata() throws Exception{
    CoordinatorEventBlockingQueue eventBlockingQueue = new CoordinatorEventBlockingQueue();
    eventBlockingQueue.put(CoordinatorEvent.LEADER_DO_ASSIGNMENT_EVENT);
    eventBlockingQueue.put(CoordinatorEvent.LEADER_DO_ASSIGNMENT_EVENT);
    eventBlockingQueue.put(CoordinatorEvent.LEADER_DO_ASSIGNMENT_EVENT);
    eventBlockingQueue.put(CoordinatorEvent.createLeaderPartitionAssignmentEvent("test1"));
    eventBlockingQueue.put(CoordinatorEvent.createLeaderPartitionAssignmentEvent("test1"));
    eventBlockingQueue.put(CoordinatorEvent.createLeaderPartitionAssignmentEvent("test2"));
    eventBlockingQueue.put(CoordinatorEvent.HANDLE_ASSIGNMENT_CHANGE_EVENT);
    eventBlockingQueue.put(CoordinatorEvent.HANDLE_ASSIGNMENT_CHANGE_EVENT);
    eventBlockingQueue.put(CoordinatorEvent.HANDLE_ASSIGNMENT_CHANGE_EVENT);
    
    Assert.assertEquals(eventBlockingQueue.take(), CoordinatorEvent.LEADER_DO_ASSIGNMENT_EVENT);
    Assert.assertEquals((String)eventBlockingQueue.take().getEventMetadata(), "test1");
    Assert.assertEquals((String)eventBlockingQueue.take().getEventMetadata(), "test1");
    Assert.assertEquals((String)eventBlockingQueue.take().getEventMetadata(), "test2");
    Assert.assertEquals(eventBlockingQueue.take(), CoordinatorEvent.HANDLE_ASSIGNMENT_CHANGE_EVENT);
  }
}
