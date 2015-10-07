package com.linkedin.datastream.server;

import com.linkedin.datastream.common.Datastream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.HashMap;
import java.util.Objects;


/**
 * DatastreamTask is the minimum assignable element of a Datastream. It is mainly used to partition the datastream
 * defined by Datastream. For example, the user can define an instance of Datastream for an Oracle bootstrap
 * connector, but this logical datastream can be splitted to a number of DatastreamTask instances, each is tied
 * to one partition. This way, each instance of DatastreamTask can be assigned independently, which in turn can
 * result in bigger output and better concurrent IO.
 *
 * <p>DatastreamTask are used as input for specific connectors. Besides the reference of the original
 * Datastream object, DatastreamTask also contains a key-value store Properties. This allows the assignment
 * strategy to attach extra parameters.
 *
 * <p>DatastreamTask has a unique name called _datastreamtaskName. This is used as the znode name in zookeeper
 * This should be unique for each instance of DatastreamTask, especially in the case when a Datastream is
 * split into multiple DatastreamTasks. This is because we will have a state associated with each DatastreamTask.
 *
 */
public class DatastreamTask {

  private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class.getName());

  // connector type. Type of the connector to be used for reading the change capture events
  // from the source, e.g. Oracle-Change, Espresso-Change, Oracle-Bootstrap, Espresso-Bootstrap,
  // Mysql-Change etc..
  private String _connectorType;

  // The Id of the datastreamtask. It is a string that will represent one assignable element of
  // datastream. By default, the value is empty string, representing that the DatastreamTask is by default
  // mapped to one Datastream. In the case when a Datastream is split into multiple partitions, the id
  // value should be the partition number. Each of the _id value will be represented in zookeeper
  // under /{cluster}/{connectorType}/{datastream}/{id}.
  private String _id = "";

  // _datastreamName is copied from Datastream instance. This is because in the znode we only persist
  // the datastream name, and only obtain instance of Datastream by reading from the znode
  // under corresponding /{cluster}/datastream/{datastreamName}
  private String _datastreamName;

  private Datastream _datastream;

  private Map<String, String> _properties = new HashMap<>();

  public DatastreamTask() {

  }

  public DatastreamTask(Datastream datastream) {
    _datastreamName = datastream.getName();
    _connectorType = datastream.getConnectorType();
    _datastream = datastream;
  }

  // construct DatastreamTask from json string
  public static DatastreamTask fromJson(String json) {
    ObjectMapper mapper = new ObjectMapper();
    DatastreamTask task = null;
    try {
      task = mapper.readValue(json, DatastreamTask.class);
    } catch (IOException ioe) {
      ioe.printStackTrace();
      LOG.error("Failed to construct DatastreamTask from json: " + json);
    }
    return task;
  }

  @JsonIgnore
  public Datastream getDatastream() {
    return _datastream;
  }

  @JsonIgnore
  public String getDatastreamTaskName() {
    return _id.equals("") ? _datastreamName : _datastreamName + "_" + _id;
  }

  public void setDatastream(Datastream datastream) {
    _datastream = datastream;
  }

  public Map<String, String> getProperties() {
    return _properties;
  }

  public void setProperties(Map<String, String> _properties) {
    this._properties = _properties;
  }

  public String getConnectorType() {
    return _connectorType;
  }

  public void setConnectorType(String connectorType) {
    _connectorType = connectorType;
  }

  public void setId(String id) {
    _id = id;
  }

  public String getId() {
    return _id;
  }

  public String getDatastreamName() {
    return _datastreamName;
  }

  public void setDatastreamName(String datastreamName) {
    this._datastreamName = datastreamName;
  }

  public String toJson() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    StringWriter out = new StringWriter(1024);
    mapper.writeValue(out, this);
    return out.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DatastreamTask task = (DatastreamTask) o;
    return Objects.equals(_connectorType, task._connectorType) &&
            Objects.equals(_id, task._id) &&
            Objects.equals(_datastreamName, task._datastreamName) &&
            Objects.equals(_datastream, task._datastream) &&
            Objects.equals(_properties, task._properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_connectorType, _id, _datastreamName, _datastream, _properties);
  }
}
