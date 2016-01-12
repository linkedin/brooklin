
package com.linkedin.datastream.diagnostics;

import java.util.List;
import javax.annotation.Generated;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.GetMode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.SetMode;


/**
 * Datastream server health
 * 
 */
@Generated(value = "com.linkedin.pegasus.generator.PegasusDataTemplateGenerator", comments = "LinkedIn Data Template. Generated from /Users/spunuru/datastream/ds/datastream-common/src/main/pegasus/com/linkedin/datastream/diagnostics/ServerHealth.pdsc.", date = "Mon Jan 11 19:13:50 PST 2016")
public class ServerHealth
    extends RecordTemplate
{

    private final static ServerHealth.Fields _fields = new ServerHealth.Fields();
    private final static RecordDataSchema SCHEMA = ((RecordDataSchema) DataTemplateUtil.parseSchema("{\"type\":\"record\",\"name\":\"ServerHealth\",\"namespace\":\"com.linkedin.datastream.diagnostics\",\"doc\":\"Datastream server health\",\"fields\":[{\"name\":\"clusterName\",\"type\":\"string\",\"doc\":\"Name of the cluster.\"},{\"name\":\"instanceName\",\"type\":\"string\",\"doc\":\"Name of the current Instance.\"},{\"name\":\"connectors\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"ConnectorHealth\",\"doc\":\"Datastream connector health\",\"fields\":[{\"name\":\"connectorType\",\"type\":\"string\",\"doc\":\"type of the connector.\"},{\"name\":\"strategy\",\"type\":\"string\",\"doc\":\"Strategy used by the connector.\"},{\"name\":\"tasks\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"TaskHealth\",\"doc\":\"Datastream connector health\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name of the task.\"},{\"name\":\"datastreams\",\"type\":\"string\",\"doc\":\"Name of the datastreams associated with the task.\"},{\"name\":\"partitions\",\"type\":\"string\",\"doc\":\"Partitions associated with the task.\"},{\"name\":\"source\",\"type\":\"string\",\"doc\":\"Source of the datastream.\"},{\"name\":\"destination\",\"type\":\"string\",\"doc\":\"Destination of the datastream.\"},{\"name\":\"status\",\"type\":\"string\",\"doc\":\"Status of the datastream task.\"},{\"name\":\"sourceCheckpoint\",\"type\":\"string\",\"doc\":\"Source checkpoint.\"}]}},\"doc\":\"tasks assigned to the connector Instance.\"}]}},\"doc\":\"Connectors that are loaded in the instance.\"}]}"));
    private final static RecordDataSchema.Field FIELD_ClusterName = SCHEMA.getField("clusterName");
    private final static RecordDataSchema.Field FIELD_InstanceName = SCHEMA.getField("instanceName");
    private final static RecordDataSchema.Field FIELD_Connectors = SCHEMA.getField("connectors");

    public ServerHealth() {
        super(new DataMap(), SCHEMA);
    }

    public ServerHealth(DataMap data) {
        super(data, SCHEMA);
    }

    public static ServerHealth.Fields fields() {
        return _fields;
    }

    /**
     * Existence checker for clusterName
     * 
     * @see Fields#clusterName
     */
    public boolean hasClusterName() {
        return contains(FIELD_ClusterName);
    }

    /**
     * Remover for clusterName
     * 
     * @see Fields#clusterName
     */
    public void removeClusterName() {
        remove(FIELD_ClusterName);
    }

    /**
     * Getter for clusterName
     * 
     * @see Fields#clusterName
     */
    public String getClusterName(GetMode mode) {
        return obtainDirect(FIELD_ClusterName, String.class, mode);
    }

    /**
     * Getter for clusterName
     * 
     * @see Fields#clusterName
     */
    public String getClusterName() {
        return getClusterName(GetMode.STRICT);
    }

    /**
     * Setter for clusterName
     * 
     * @see Fields#clusterName
     */
    public ServerHealth setClusterName(String value, SetMode mode) {
        putDirect(FIELD_ClusterName, String.class, String.class, value, mode);
        return this;
    }

    /**
     * Setter for clusterName
     * 
     * @see Fields#clusterName
     */
    public ServerHealth setClusterName(String value) {
        putDirect(FIELD_ClusterName, String.class, String.class, value, SetMode.DISALLOW_NULL);
        return this;
    }

    /**
     * Existence checker for instanceName
     * 
     * @see Fields#instanceName
     */
    public boolean hasInstanceName() {
        return contains(FIELD_InstanceName);
    }

    /**
     * Remover for instanceName
     * 
     * @see Fields#instanceName
     */
    public void removeInstanceName() {
        remove(FIELD_InstanceName);
    }

    /**
     * Getter for instanceName
     * 
     * @see Fields#instanceName
     */
    public String getInstanceName(GetMode mode) {
        return obtainDirect(FIELD_InstanceName, String.class, mode);
    }

    /**
     * Getter for instanceName
     * 
     * @see Fields#instanceName
     */
    public String getInstanceName() {
        return getInstanceName(GetMode.STRICT);
    }

    /**
     * Setter for instanceName
     * 
     * @see Fields#instanceName
     */
    public ServerHealth setInstanceName(String value, SetMode mode) {
        putDirect(FIELD_InstanceName, String.class, String.class, value, mode);
        return this;
    }

    /**
     * Setter for instanceName
     * 
     * @see Fields#instanceName
     */
    public ServerHealth setInstanceName(String value) {
        putDirect(FIELD_InstanceName, String.class, String.class, value, SetMode.DISALLOW_NULL);
        return this;
    }

    /**
     * Existence checker for connectors
     * 
     * @see Fields#connectors
     */
    public boolean hasConnectors() {
        return contains(FIELD_Connectors);
    }

    /**
     * Remover for connectors
     * 
     * @see Fields#connectors
     */
    public void removeConnectors() {
        remove(FIELD_Connectors);
    }

    /**
     * Getter for connectors
     * 
     * @see Fields#connectors
     */
    public ConnectorHealthArray getConnectors(GetMode mode) {
        return obtainWrapped(FIELD_Connectors, ConnectorHealthArray.class, mode);
    }

    /**
     * Getter for connectors
     * 
     * @see Fields#connectors
     */
    public ConnectorHealthArray getConnectors() {
        return getConnectors(GetMode.STRICT);
    }

    /**
     * Setter for connectors
     * 
     * @see Fields#connectors
     */
    public ServerHealth setConnectors(ConnectorHealthArray value, SetMode mode) {
        putWrapped(FIELD_Connectors, ConnectorHealthArray.class, value, mode);
        return this;
    }

    /**
     * Setter for connectors
     * 
     * @see Fields#connectors
     */
    public ServerHealth setConnectors(ConnectorHealthArray value) {
        putWrapped(FIELD_Connectors, ConnectorHealthArray.class, value, SetMode.DISALLOW_NULL);
        return this;
    }

    @Override
    public ServerHealth clone()
        throws CloneNotSupportedException
    {
        return ((ServerHealth) super.clone());
    }

    @Override
    public ServerHealth copy()
        throws CloneNotSupportedException
    {
        return ((ServerHealth) super.copy());
    }

    public static class Fields
        extends PathSpec
    {


        public Fields(List<String> path, String name) {
            super(path, name);
        }

        public Fields() {
            super();
        }

        /**
         * Name of the cluster.
         * 
         */
        public PathSpec clusterName() {
            return new PathSpec(getPathComponents(), "clusterName");
        }

        /**
         * Name of the current Instance.
         * 
         */
        public PathSpec instanceName() {
            return new PathSpec(getPathComponents(), "instanceName");
        }

        /**
         * Connectors that are loaded in the instance.
         * 
         */
        public com.linkedin.datastream.diagnostics.ConnectorHealthArray.Fields connectors() {
            return new com.linkedin.datastream.diagnostics.ConnectorHealthArray.Fields(getPathComponents(), "connectors");
        }

    }

}
