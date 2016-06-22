package com.linkedin.datastream.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.I0Itec.zkclient.IZkChildListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.server.zk.KeyBuilder;


/**
 * Class that maintains the cache of all the datastreams in the datastream cluster.
 *
 * List of all the datastream names are always kept up-to date. But the complete datastream objects are lazily read
 * from the zookeeper when they are requested.
 *
 * Note : This layer assumes that the datastreams are not updated once they are created.
 */
public class CachedDatastreamReader {

  private static final Logger LOG = LoggerFactory.getLogger(CachedDatastreamReader.class.getName());

  private final String _cluster;
  private List<String> _datastreamList = Collections.emptyList();
  private Map<String, Datastream> _datastreams = new HashMap<>();
  private final ZkClient _zkclient;

  public CachedDatastreamReader(ZkClient zkclient, String cluster) {
    _zkclient = zkclient;
    _cluster = cluster;

    // Get the initial datastream list.
    _datastreamList = fetchAllDatastreamNamesFromZk();

    String path = KeyBuilder.datastreams(_cluster);
    LOG.info("Subscribing to notification on zk path " + path);

    // Be notified of changes to the children list in order to cache it. The listener creates a copy of the list
    // because other listeners could potentially get access to it and modify its contents.
    _zkclient.subscribeChildChanges(path, new IZkChildListener() {
      @Override
      public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
        synchronized (CachedDatastreamReader.this) {
          LOG.debug(
              String.format("Received datastream add or delete notification. parentPath %s, children %s", parentPath,
                  currentChildren));
          _datastreamList =
              currentChildren.stream().collect(Collectors.toCollection(() -> new ArrayList<>(currentChildren.size())));
          List<String> datastreamsRemoved =
              _datastreams.keySet().stream().filter(x -> !_datastreamList.contains(x)).collect(Collectors.toList());

          if (!datastreamsRemoved.isEmpty()) {
            LOG.info(String.format("Removing the deleted datastreams {%s} from cache", datastreamsRemoved));
            datastreamsRemoved.stream().forEach(_datastreams::remove);
          }

          LOG.debug(String.format("New datastream list in the cache: %s", _datastreamList));
        }
      }
    });
  }

  public synchronized List<String> getAllDatastreamNames() {
    return Collections.unmodifiableList(_datastreamList);
  }

  public synchronized List<Datastream> getAllDatastreams() {
    return getAllDatastreams(false);
  }

  public List<Datastream> getAllDatastreams(boolean flushCache) {
    if (flushCache) {
      _datastreamList = fetchAllDatastreamNamesFromZk();
    }

    return _datastreamList.stream().map(this::getDatastream).collect(Collectors.toList());
  }

  public Datastream getDatastream(String datastreamName) {
    Datastream ds = _datastreams.get(datastreamName);
    if (ds == null) {
      ds = getDatastreamFromZk(datastreamName);

      // If it has destination connection string populated then it is a complete datastream, cache it.
      if (ds.hasDestination() && ds.getDestination().hasConnectionString()) {
        _datastreams.put(datastreamName, ds);
      }
    }
    return ds;
  }

  private Datastream getDatastreamFromZk(String datastreamName) {
    String path = KeyBuilder.datastream(_cluster, datastreamName);
    String content = _zkclient.ensureReadData(path);
    return DatastreamUtils.fromJSON(content);
  }

  private List<String> fetchAllDatastreamNamesFromZk() {
    String path = KeyBuilder.datastreams(_cluster);
    _zkclient.ensurePath(path);
    return _zkclient.getChildren(path, true);
  }
}
