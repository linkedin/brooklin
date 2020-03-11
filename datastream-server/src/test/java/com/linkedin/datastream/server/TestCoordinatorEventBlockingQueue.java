/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests for {@link CoordinatorEventBlockingQueue}
 */
public class TestCoordinatorEventBlockingQueue {

  @Test
  public void testHappyPath() throws Exception {
    CoordinatorEventBlockingQueue eventBlockingQueue = new CoordinatorEventBlockingQueue();
    eventBlockingQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(false));
    eventBlockingQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(true));
    eventBlockingQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(false));
    eventBlockingQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(true));
    eventBlockingQueue.put(CoordinatorEvent.createLeaderPartitionAssignmentEvent("test1"));
    eventBlockingQueue.put(CoordinatorEvent.createLeaderPartitionAssignmentEvent("test1"));
    eventBlockingQueue.put(CoordinatorEvent.createLeaderPartitionAssignmentEvent("test2"));
    eventBlockingQueue.put(CoordinatorEvent.HANDLE_ASSIGNMENT_CHANGE_EVENT);
    eventBlockingQueue.put(CoordinatorEvent.HANDLE_ASSIGNMENT_CHANGE_EVENT);
    eventBlockingQueue.put(CoordinatorEvent.HANDLE_ASSIGNMENT_CHANGE_EVENT);
    Assert.assertEquals(eventBlockingQueue.take(), CoordinatorEvent.createLeaderDoAssignmentEvent(false));
    Assert.assertEquals(eventBlockingQueue.take(), CoordinatorEvent.createLeaderDoAssignmentEvent(true));
    Assert.assertEquals(eventBlockingQueue.take(), CoordinatorEvent.createLeaderPartitionAssignmentEvent("test1"));
    Assert.assertEquals(eventBlockingQueue.take(), CoordinatorEvent.createLeaderPartitionAssignmentEvent("test2"));
    Assert.assertEquals(eventBlockingQueue.take(), CoordinatorEvent.HANDLE_ASSIGNMENT_CHANGE_EVENT);
  }
}
