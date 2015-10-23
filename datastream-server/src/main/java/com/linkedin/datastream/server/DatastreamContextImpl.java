package com.linkedin.datastream.server;

import com.linkedin.datastream.server.zk.ZkAdapter;


public class DatastreamContextImpl implements DatastreamContext {

  private ZkAdapter _adapter;

  public DatastreamContextImpl(ZkAdapter adapter) {
    _adapter = adapter;
  }

  @Override
  public String getState(DatastreamTask datastreamTask, String key) {
    return _adapter.getDatastreamTaskStateForKey(datastreamTask, key);
  }

  @Override
  public void saveState(DatastreamTask datastreamTask, String key, String value) {
    _adapter.setDatastreamTaskStateForKey(datastreamTask, key, value);
  }

  @Override
  public String getInstanceName() {
    return _adapter.getInstanceName();
  }
}
