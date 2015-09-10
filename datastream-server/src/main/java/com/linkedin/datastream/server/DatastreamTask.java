package com.linkedin.datastream.server;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamJSonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.HashMap;

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
 */
public class DatastreamTask {

    private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class.getName());

    // connector type. Type of the connector to be used for reading the change capture events
    // from the source, e.g. Oracle-Change, Espresso-Change, Oracle-Bootstrap, Espresso-Bootstrap,
    // Mysql-Change etc..
    // All datastreams wrapped in one assignable DatastreamTask must belong to the same connector type.
    private String _connectorType;
    private String _datastreamName;
    private String _taskName;

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
        try{
            task = mapper.readValue(json, DatastreamTask.class);
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
            LOG.error("Failed to construct DatastreamTask from json: " + json);
        }
        return task;
    }

    @JsonIgnore
    public Datastream getDatastream() {
        return _datastream;
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

    public String getTaskName() {
        return _taskName;
    }

    public void setTaskName(String _taskName) {
        this._taskName = _taskName;
    }

    public String getDatastreamName() {
        return _datastreamName;
    }

    public void setDatastreamName(String _datastreamName) {
        this._datastreamName = _datastreamName;
    }

    @JsonIgnore
    public String getName() { return  _taskName == null ? _datastreamName : _datastreamName + "_" + _taskName; }

    public String toJson() throws IOException{
        ObjectMapper mapper = new ObjectMapper();
        StringWriter out = new StringWriter(1024);
        mapper.writeValue(out, this);
        return out.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof DatastreamTask)) {
            return false;
        }

        DatastreamTask other = (DatastreamTask) obj;
        return other.getName() == this.getName();
    }
}