/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.dms;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamAlreadyExistsException;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamStatus;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.server.CachedDatastreamReader;
import com.linkedin.datastream.server.TargetAssignment;
import com.linkedin.datastream.server.zk.KeyBuilder;

/**
 * ZooKeeper-backed {@link DatastreamStore}
 */
public class ZookeeperBackedDatastreamStore implements DatastreamStore {

  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperBackedDatastreamStore.class.getName());

  private final ZkClient _zkClient;
  private final String _cluster;
  private final CachedDatastreamReader _datastreamCache;

  /**
   * Construct an instance of ZookeeperBackedDatastreamStore
   * @param datastreamCache cache for datastream data
   * @param zkClient ZooKeeper client to use
   * @param cluster Brooklin cluster name
   */
  public ZookeeperBackedDatastreamStore(CachedDatastreamReader datastreamCache, ZkClient zkClient, String cluster) {
    Validate.notNull(datastreamCache);
    Validate.notNull(zkClient);
    Validate.notNull(cluster);

    _datastreamCache = datastreamCache;
    _zkClient = zkClient;
    _cluster = cluster;
  }

  private String getZnodePath(String key) {
    return KeyBuilder.datastream(_cluster, key);
  }

  private List<String> getInstances() {
    return _zkClient.getChildren(KeyBuilder.liveInstances(_cluster));
  }

  @Override
  public Datastream getDatastream(String key) {
    if (key == null) {
      return null;
    }
    String path = getZnodePath(key);
    String json = _zkClient.readData(path, true /* returnNullIfPathNotExists */);
    if (json == null) {
      return null;
    }
    return DatastreamUtils.fromJSON(json);
  }

  /**
   * Retrieves all the datastreams in the store. Since there may be many datastreams, it is better
   * to return a Stream and enable further filtering and transformation rather that just a List.
   *
   * The datastream key-set used to make this call is cached, it is possible to get a slightly outdated
   * list of datastreams and not have a stream that was just added. It depends on how long it takes for
   * ZooKeeper to notify the change.
   */
  @Override
  public Stream<String> getAllDatastreams() {
    return _datastreamCache.getAllDatastreamNames().stream().sorted();
  }

  @Override
  public void updateDatastream(String key, Datastream datastream, boolean notifyLeader) throws DatastreamException {
    Datastream oldDatastream = getDatastream(key);
    if (oldDatastream == null) {
      throw new DatastreamException("Datastream does not exists, can not be updated: " + key);
    }

    String json = DatastreamUtils.toJSON(datastream);
    _zkClient.writeData(getZnodePath(key), json);
    if (notifyLeader) {
      notifyLeaderOfDataChange();
    }
  }

  @Override
  public void createDatastream(String key, Datastream datastream) {
    Validate.notNull(datastream, "null datastream");
    Validate.notNull(key, "null key for datastream" + datastream);

    String path = getZnodePath(key);
    if (_zkClient.exists(path)) {
      String content = _zkClient.ensureReadData(path);
      String errorMessage = String.format("Datastream already exists: path=%s, content=%s", key, content);
      LOG.warn(errorMessage);
      throw new DatastreamAlreadyExistsException(errorMessage);
    }
    _zkClient.ensurePath(path);
    String json = DatastreamUtils.toJSON(datastream);
    _zkClient.writeData(path, json);
  }

  @Override
  public void updatePartitionAssignments(String key, Datastream datastream, TargetAssignment targetAssignment)
      throws DatastreamException {
    Validate.notNull(datastream, "null datastream");
    Validate.notNull(key, "null key for datastream" + datastream);
    verifyHostname(targetAssignment.getTargetHost());

    long current = System.currentTimeMillis();
    String datastreamGroupName = DatastreamUtils.getTaskPrefix(datastream);
    String path = KeyBuilder.getTargetAssignment(_cluster, datastreamGroupName);
    _zkClient.ensurePath(path);
    if (_zkClient.exists(path)) {
      String json = targetAssignment.toJson();
      _zkClient.ensurePath(path + '/' + current);
      _zkClient.writeData(path + '/' + current, json);
    }

    try {
      _zkClient.writeData(KeyBuilder.getTargetAssignmentBase(_cluster), String.valueOf(System.currentTimeMillis()));
    } catch (Exception e) {
      // we don't need to do an atomic update; if the node gets update by others somehow or get deleted by
      // leader, it's ok to ignore the failure
      LOG.warn("Failed to touch the assignment update", e);
    }
  }

  private void verifyHostname(String hostname) throws DatastreamException {
    try {
      String path = KeyBuilder.instances(_cluster);
      _zkClient.ensurePath(path);
      List<String> instances = _zkClient.getChildren(path);
      Set<String> hostnames = instances.stream().map(s -> s.substring(0, s.lastIndexOf('-'))).collect(Collectors.toSet());
      if (!hostnames.contains(hostname)) {
        throw new DatastreamException("Hostname " + hostname + " is not valid");
      }
    } catch (Exception ex) {
      throw new DatastreamException(ex);
    }
  }

  @Override
  public void deleteDatastream(String key) {
    Validate.notNull(key, "null key");

    Datastream datastream = getDatastream(key);
    if (datastream != null) {
      datastream.setStatus(DatastreamStatus.DELETING);
      String data = DatastreamUtils.toJSON(datastream);
      String path = getZnodePath(key);
      _zkClient.updateDataSerialized(path, old -> data);
      notifyLeaderOfDataChange();
    }
  }

  private void notifyLeaderOfDataChange() {
    String dmsPath = KeyBuilder.datastreams(_cluster);
    // Update the /dms to notify that coordinator needs to act on a deleted or changed datastream.
    _zkClient.updateDataSerialized(dmsPath, old -> String.valueOf(System.currentTimeMillis()));
  }
}
