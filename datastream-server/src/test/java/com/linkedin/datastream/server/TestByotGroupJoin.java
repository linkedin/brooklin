/**
 *  Copyright 2026 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.DatastreamStatus;
import com.linkedin.datastream.server.assignment.BroadcastStrategyFactory;
import com.linkedin.datastream.server.assignment.StickyPartitionAssignmentStrategy;

import static com.linkedin.datastream.server.DatastreamServerConfigurationConstants.CONFIG_CONNECTOR_ALLOW_BYOT_GROUP_JOIN;


/**
 * Tests for {@link Coordinator#findByotGroupJoinConflict}
 */
public class TestByotGroupJoin {
  private static final String PREFIX = "group1";
  private static final Predicate<Datastream> NEVER_EXPIRED = ds -> false;

  /**
   * Generates a BYOT datastream of {@link #PREFIX}'s group, so any two of them can join each other
   */
  private static Datastream byot(String name, DatastreamStatus status) {
    Datastream ds = new Datastream();
    ds.setName(name);
    ds.setConnectorName("test");
    ds.setTransportProviderName("transport");
    ds.setSource(new DatastreamSource());
    ds.getSource().setConnectionString("source");
    ds.getSource().setPartitions(4);
    ds.setDestination(new DatastreamDestination());
    ds.getDestination().setConnectionString("topic");
    ds.getDestination().setPartitions(8);
    ds.getDestination().setKeySerDe("keySerde");
    ds.getDestination().setPayloadSerDe("payloadSerde");
    ds.getDestination().setEnvelopeSerDe("envelopeSerde");
    if (status != null) {
      ds.setStatus(status);
    }
    ds.setMetadata(new StringMap());
    ds.getMetadata().put(DatastreamMetadataConstants.OWNER_KEY, "owner");
    ds.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, PREFIX);
    ds.getMetadata().put(DatastreamMetadataConstants.IS_USER_MANAGED_DESTINATION_KEY, "true");
    return ds;
  }

  private static Datastream member(String name) {
    return byot(name, DatastreamStatus.READY);
  }

  private static Datastream joiner() {
    return byot("joiner", DatastreamStatus.INITIALIZING);
  }

  private static String conflict(Datastream joiner, Datastream... members) {
    return Coordinator.findByotGroupJoinConflict(joiner, Arrays.asList(members), NEVER_EXPIRED);
  }

  private static void assertRefused(String conflict, String expectedName) {
    Assert.assertNotNull(conflict, "The join should have been refused");
    Assert.assertTrue(conflict.contains(expectedName), conflict + " should name " + expectedName);
  }

  @Test
  public void testKeyName() {
    Assert.assertEquals(CONFIG_CONNECTOR_ALLOW_BYOT_GROUP_JOIN, "allowByotGroupJoin");
  }

  @Test
  public void testJoinsReadyMember() {
    Assert.assertNull(conflict(joiner(), member("m1")));
  }

  @Test
  public void testJoinsInitializingMember() {
    Assert.assertNull(conflict(joiner(), byot("m1", DatastreamStatus.INITIALIZING)));
  }

  @Test
  public void testJoinsGroupOfSeveralMembers() {
    Assert.assertNull(conflict(joiner(), member("m1"), byot("m2", DatastreamStatus.INITIALIZING)));
  }

  @Test
  public void testIgnoresReuseExistingDestination() {
    Datastream m1 = member("m1");
    m1.getMetadata().put(DatastreamMetadataConstants.REUSE_EXISTING_DESTINATION_KEY, "false");
    Datastream joiner = joiner();
    joiner.getMetadata().put(DatastreamMetadataConstants.REUSE_EXISTING_DESTINATION_KEY, "false");
    Assert.assertNull(conflict(joiner, m1));
  }

  @Test
  public void testRefusesWithoutTaskPrefix() {
    Datastream noPrefix = joiner();
    noPrefix.getMetadata().remove(DatastreamMetadataConstants.TASK_PREFIX);
    assertRefused(conflict(noPrefix, member("m1")), "joiner");

    Datastream blankPrefix = joiner();
    blankPrefix.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, "  ");
    assertRefused(conflict(blankPrefix, member("m1")), "joiner");
  }

  @Test
  public void testRefusesMemberOfAnotherGroup() {
    Datastream other = member("m1");
    other.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, "group2");
    assertRefused(conflict(joiner(), other), "m1");

    Datastream noPrefix = member("m2");
    noPrefix.getMetadata().remove(DatastreamMetadataConstants.TASK_PREFIX);
    assertRefused(conflict(joiner(), noPrefix), "m2");
  }

  @Test
  public void testRefusesTopicSharedByTwoGroups() {
    Datastream other = member("m2");
    other.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, "group2");
    assertRefused(conflict(joiner(), member("m1"), other), "m2");
  }

  @Test
  public void testRefusesMemberThatIsNotByot() {
    Datastream notByot = member("m1");
    notByot.getMetadata().remove(DatastreamMetadataConstants.IS_USER_MANAGED_DESTINATION_KEY);
    assertRefused(conflict(joiner(), notByot), "m1");
  }

  @Test
  public void testRefusesDifferentSource() {
    Datastream differentConnectionString = member("m1");
    differentConnectionString.getSource().setConnectionString("otherSource");
    assertRefused(conflict(joiner(), differentConnectionString), "m1");

    Datastream differentPartitions = member("m2");
    differentPartitions.getSource().setPartitions(5);
    assertRefused(conflict(joiner(), differentPartitions), "m2");
  }

  @Test
  public void testRefusesDifferentDestinationPartitions() {
    Datastream m1 = member("m1");
    m1.getDestination().setPartitions(9);
    assertRefused(conflict(joiner(), m1), "m1");
  }

  @Test
  public void testRefusesDifferentTransportProvider() {
    Datastream m1 = member("m1");
    m1.setTransportProviderName("otherTransport");
    assertRefused(conflict(joiner(), m1), "m1");
  }

  @DataProvider(name = "serdes")
  public Object[][] serdes() {
    return new Object[][]{{"key"}, {"payload"}, {"envelope"}};
  }

  @Test(dataProvider = "serdes")
  public void testRefusesDifferentSerdes(String serde) {
    Datastream m1 = member("m1");
    DatastreamDestination destination = m1.getDestination();
    if ("key".equals(serde)) {
      destination.setKeySerDe("otherSerde");
    } else if ("payload".equals(serde)) {
      destination.setPayloadSerDe("otherSerde");
    } else {
      destination.setEnvelopeSerDe("otherSerde");
    }
    assertRefused(conflict(joiner(), m1), "m1");
  }

  @DataProvider(name = "notJoinableStatuses")
  public Object[][] notJoinableStatuses() {
    return new Object[][]{{DatastreamStatus.STOPPED}, {DatastreamStatus.STOPPING}, {DatastreamStatus.PAUSED},
        {DatastreamStatus.DELETING}, {null}};
  }

  @Test(dataProvider = "notJoinableStatuses")
  public void testRefusesMemberThatIsNotInitializingOrReady(DatastreamStatus status) {
    assertRefused(conflict(joiner(), byot("m1", status)), "m1");
  }

  @Test
  public void testRefusesMemberThatTheCoordinatorSaysIsDeletingOrExpired() {
    Datastream ready = member("ready");
    Datastream initializing = byot("initializing", DatastreamStatus.INITIALIZING);
    List<Datastream> members = Arrays.asList(ready, initializing);

    assertRefused(Coordinator.findByotGroupJoinConflict(joiner(), members, ds -> ds == ready), "ready");
    assertRefused(Coordinator.findByotGroupJoinConflict(joiner(), members, ds -> ds == initializing), "initializing");
    Assert.assertNull(Coordinator.findByotGroupJoinConflict(joiner(), members, NEVER_EXPIRED));
  }

  @Test
  public void testRefusalNamesTheConflictingMember() {
    Datastream other = member("m2");
    other.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, "group2");
    Assert.assertEquals(conflict(joiner(), member("m1"), other), "m2 is in another group");
  }

  @Test
  public void testDoesNotModifyTheStreams() throws Exception {
    Datastream joiner = joiner();
    Datastream m1 = member("m1");
    Datastream stopped = byot("m2", DatastreamStatus.STOPPED);
    Datastream joinerBefore = joiner.copy();
    Datastream m1Before = m1.copy();
    Datastream stoppedBefore = stopped.copy();

    Assert.assertNull(conflict(joiner, m1));
    Assert.assertNotNull(conflict(joiner, m1, stopped));

    Assert.assertEquals(joiner, joinerBefore);
    Assert.assertEquals(m1, m1Before);
    Assert.assertEquals(stopped, stoppedBefore);
  }

  @DataProvider(name = "taskCounts")
  public Object[][] taskCounts() {
    String[] keys = {BroadcastStrategyFactory.CFG_MAX_TASKS, StickyPartitionAssignmentStrategy.CFG_MIN_TASKS};
    Object[][] cases = new Object[keys.length * 4][];
    int i = 0;
    for (String key : keys) {
      cases[i++] = new Object[]{key, "3", null};
      cases[i++] = new Object[]{key, null, "3"};
      cases[i++] = new Object[]{key, "3", "4"};
      cases[i++] = new Object[]{key, "3", ""};
    }
    return cases;
  }

  @Test(dataProvider = "taskCounts")
  public void testRefusesDifferentTaskCounts(String key, String memberValue, String joinerValue) {
    Datastream m1 = member("m1");
    Datastream joiner = joiner();
    if (memberValue != null) {
      m1.getMetadata().put(key, memberValue);
    }
    if (joinerValue != null) {
      joiner.getMetadata().put(key, joinerValue);
    }
    assertRefused(conflict(joiner, m1), "m1");
  }

  @Test
  public void testJoinsWithTheSameTaskCounts() {
    Datastream m1 = member("m1");
    Datastream joiner = joiner();
    for (Datastream ds : Arrays.asList(m1, joiner)) {
      ds.getMetadata().put(BroadcastStrategyFactory.CFG_MAX_TASKS, "3");
      ds.getMetadata().put(StickyPartitionAssignmentStrategy.CFG_MIN_TASKS, "2");
    }
    Assert.assertNull(conflict(joiner, m1));
  }
}
