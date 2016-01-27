package com.linkedin.datastream.server.dms;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.server.zk.KeyBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class ZookeeperBackedDatastreamStore implements DatastreamStore {

  private final ZkClient _zkClient;
  private final String _rootPath;
  // Cache of the datastream key list
  private volatile List<String> _datastreamList = Collections.emptyList();

  public ZookeeperBackedDatastreamStore(ZkClient zkClient, String cluster) {
    assert zkClient != null;
    assert cluster != null;

    _zkClient = zkClient;
    _rootPath = KeyBuilder.datastreams(cluster);

    // Be notified of changes to the children list in order to cache it. The listener creates a copy of the list
    // because other listeners could potentially get access to it and modify its contents.
    _zkClient.subscribeChildChanges(_rootPath, (parentPath, currentChildren) ->
        _datastreamList = currentChildren.stream().collect(Collectors.toCollection(() -> new ArrayList<>(currentChildren.size()))));
  }

  private String getZnodePath(String key) {
    return _rootPath + "/" + key;
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
   *
   * @return
   */
  @Override
  public Stream<String> getAllDatastreams() {
    return _datastreamList.stream().sorted();
  }

  @Override
  public boolean updateDatastream(String key, Datastream datastream) {
    // Updating a Datastream is still tricky for now. Changing either the
    // the source or target may result in failure on connector.
    // We could possibly only allow updates on metadata field
    return false;
  }

  @Override
  public boolean createDatastream(String key, Datastream datastream) {
    if (key == null || datastream == null) {
      return false;
    }
    String path = getZnodePath(key);
    if (_zkClient.exists(path)) {
      return false;
    }
    _zkClient.ensurePath(path);
    String json = DatastreamUtils.toJSON(datastream);
    _zkClient.writeData(path, json);
    return true;
  }

  @Override
  public boolean deleteDatastream(String key) {
    if (key == null) {
      return false;
    }
    String path = getZnodePath(key);
    if (!_zkClient.exists(path)) {
      // delete operation is idempotent
      return true;
    }
    _zkClient.delete(getZnodePath(key));
    return true;
  }
}
