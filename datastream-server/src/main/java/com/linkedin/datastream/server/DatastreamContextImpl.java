package com.linkedin.datastream.server;

import com.linkedin.datastream.server.zk.ZkAdapter;


public class DatastreamContextImpl implements DatastreamContext {

  private static final String STATUS = "STATUS";

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

  @Override
  public void setStatus(DatastreamTask datastreamTask, DatastreamTaskStatus status) {
    saveState(datastreamTask, STATUS, status.toString());
  }

  @Override
  public DatastreamTaskStatus getStatus(DatastreamTask datastreamTask) {
    String statusStr = this.getState(datastreamTask, STATUS);
    return DatastreamTaskStatus.valueOf(statusStr);
  }
}
