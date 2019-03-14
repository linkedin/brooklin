/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.server.zk.KeyBuilder;


/**
 * Class that maintains the cache of all the datastreams in the datastream cluster.
 *
 * List of all the datastream names are always kept up-to date (barring ZK watcher delay).
 * But the complete datastream objects are lazily read from Zookeeper when they are
 * requested.
 *
 * Note: Caller of this class is expected to call invalidateAllCache for any datastream
 * update events such that any future datastream accesses will update the cached copies.
 */
public class CachedDatastreamReader {
  private static final Logger LOG = LoggerFactory.getLogger(CachedDatastreamReader.class);

  private final String _cluster;
  private final ZkClient _zkclient;

  private List<String> _datastreamNames;
  private Map<String, Datastream> _datastreams = new ConcurrentHashMap<>();

  public CachedDatastreamReader(ZkClient zkclient, String cluster) {
    _zkclient = zkclient;
    _cluster = cluster;

    // Get the initial datastream list.
    _datastreamNames = fetchAllDatastreamNamesFromZk();

    String path = KeyBuilder.datastreams(_cluster);
    LOG.info("Subscribing to notification on zk path " + path);

    // Be notified of changes to the children list in order to cache it. The listener creates a copy of the list
    // because other listeners could potentially get access to it and modify its contents.
    _zkclient.subscribeChildChanges(path, (parentPath, currentChildren) -> {
      synchronized (CachedDatastreamReader.this) {
        LOG.debug(
            String.format("Received datastream add or delete notification. parentPath %s, children %s", parentPath,
                currentChildren));
        _datastreamNames = new ArrayList<>(currentChildren);
        Set<String> datastreamsRemoved = new HashSet<>(_datastreams.keySet());
        datastreamsRemoved.removeAll(_datastreamNames);
        if (!datastreamsRemoved.isEmpty()) {
          LOG.info("Removing the deleted datastreams {} from cache", datastreamsRemoved);
          _datastreams.keySet().removeAll(datastreamsRemoved);
        }

        LOG.debug("New datastream list in the cache: {}", _datastreamNames);
      }
    });
  }

  /**
   * Get the current datastream groups in the cache (no calls to ZK).
   */
  public synchronized List<DatastreamGroup> getDatastreamGroups() {
    List<Datastream> allStreams = getAllDatastreams(false);

    Map<String, List<Datastream>> streamsByTaskPrefix = allStreams.stream()
        .filter(DatastreamUtils::containsTaskPrefix)
        .collect(Collectors.groupingBy(DatastreamUtils::getTaskPrefix, Collectors.toList()));

    return streamsByTaskPrefix.keySet()
        .stream()
        .map(x -> new DatastreamGroup(streamsByTaskPrefix.get(x)))
        .collect(Collectors.toList());
  }

  /**
   * Get the current list of datastream names in the cache (no calls to ZK).
   */
  public synchronized List<String> getAllDatastreamNames() {
    return Collections.unmodifiableList(_datastreamNames);
  }

  /**
   * Get the current list of datastreams in the cache (no calls to ZK).
   */
  public synchronized List<Datastream> getAllDatastreams() {
    return getAllDatastreams(false);
  }

  /**
   * Get the current list of datastreams in the cache. Caveat: if flushCache
   * is false, there could be a very short window (ZK watcher latency) that
   * the returned list is out-of-sync with ZK. Caller should be aware of this.
   * @param flushCache if true, all datastreams should be refetched from ZK
   */
  public synchronized List<Datastream> getAllDatastreams(boolean flushCache) {
    if (flushCache) {
      _datastreamNames = fetchAllDatastreamNamesFromZk();
    }

    return _datastreamNames.stream()
        .map(x -> this.getDatastream(x, flushCache))
        .filter(Objects::nonNull) // getDatastream can return null when the stream is just deleted in ZK
        .collect(Collectors.toList());
  }

  /**
   * Invalidate all cache entries to force the reader to get fresh copy of the data from zk.
   * While the list of datastreams is mostly up-to-date (zk watcher delay), there is no guarantee
   * that the CacheDatastreamReader is keeping a fresh copy of the actual content. Calling this
   * function would effectively make sure any following getDatastream calls get a newer copy of data.
   */
  public synchronized void invalidateAllCache() {
    LOG.info("About to invalidate all cache entries...");
    _datastreams.clear();
  }

  /**
   * Lookup the cached datastream based on its name with the option to access ZK for latest copy.
   * @param datastreamName name of the datastream
   * @param flushCache whether zk should be accessed regardless of cache hits
   * @return the datastream object if exists; or null not exists in either cache or ZK
   */
  @VisibleForTesting
  Datastream getDatastream(String datastreamName, boolean flushCache) {
    Datastream ds = _datastreams.get(datastreamName);
    if (ds == null || flushCache) {
      ds = getDatastreamFromZk(datastreamName);

      if (ds == null) {
        LOG.info("Datastream {} does not exist in cache/ZK.", datastreamName);
      } else if (!DatastreamUtils.hasValidDestination(ds)) {
        LOG.info("Datastream {} does not have a valid destination yet and is not ready for use.", datastreamName);
      } else {
        _datastreams.put(datastreamName, ds);
      }
    }
    return ds;
  }

  /**
   * Lookup the datastream based on its name from ZK.
   * @param datastreamName name of the datastream
   * @return the datastream object if exists; or null not exists in ZK
   */
  private Datastream getDatastreamFromZk(String datastreamName) {
    String path = KeyBuilder.datastream(_cluster, datastreamName);
    if (_zkclient.exists(path)) {
      try {
        String content = _zkclient.ensureReadData(path);
        if (content != null) {
          return DatastreamUtils.fromJSON(content);
        }
      } catch (ZkNoNodeException e) {
        // This can happen when the path still exists but later deleted
        // during ensureReadData
        LOG.warn("Datastream {} is just deleted from ZK.", datastreamName);
      }
    }
    return null;
  }

  private List<String> fetchAllDatastreamNamesFromZk() {
    String path = KeyBuilder.datastreams(_cluster);
    if (_zkclient.exists(path)) {
      return _zkclient.getChildren(path, true);
    } else {
      LOG.warn("Brooklin cluster '{}' is not initialize yet.", _cluster);
      return Collections.emptyList();
    }
  }
}
