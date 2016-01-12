
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
 * Datastream connector health
 * 
 */
@Generated(value = "com.linkedin.pegasus.generator.PegasusDataTemplateGenerator", comments = "LinkedIn Data Template. Generated from /Users/spunuru/datastream/ds/datastream-common/src/main/pegasus/com/linkedin/datastream/diagnostics/ConnectorHealth.pdsc.", date = "Mon Jan 11 19:13:50 PST 2016")
public class ConnectorHealth
    extends RecordTemplate
{

    private final static ConnectorHealth.Fields _fields = new ConnectorHealth.Fields();
    private final static RecordDataSchema SCHEMA = ((RecordDataSchema) DataTemplateUtil.parseSchema("{\"type\":\"record\",\"name\":\"ConnectorHealth\",\"namespace\":\"com.linkedin.datastream.diagnostics\",\"doc\":\"Datastream connector health\",\"fields\":[{\"name\":\"connectorType\",\"type\":\"string\",\"doc\":\"type of the connector.\"},{\"name\":\"strategy\",\"type\":\"string\",\"doc\":\"Strategy used by the connector.\"},{\"name\":\"tasks\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"TaskHealth\",\"doc\":\"Datastream connector health\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name of the task.\"},{\"name\":\"datastreams\",\"type\":\"string\",\"doc\":\"Name of the datastreams associated with the task.\"},{\"name\":\"partitions\",\"type\":\"string\",\"doc\":\"Partitions associated with the task.\"},{\"name\":\"source\",\"type\":\"string\",\"doc\":\"Source of the datastream.\"},{\"name\":\"destination\",\"type\":\"string\",\"doc\":\"Destination of the datastream.\"},{\"name\":\"status\",\"type\":\"string\",\"doc\":\"Status of the datastream task.\"},{\"name\":\"sourceCheckpoint\",\"type\":\"string\",\"doc\":\"Source checkpoint.\"}]}},\"doc\":\"tasks assigned to the connector Instance.\"}]}"));
    private final static RecordDataSchema.Field FIELD_ConnectorType = SCHEMA.getField("connectorType");
    private final static RecordDataSchema.Field FIELD_Strategy = SCHEMA.getField("strategy");
    private final static RecordDataSchema.Field FIELD_Tasks = SCHEMA.getField("tasks");

    public ConnectorHealth() {
        super(new DataMap(), SCHEMA);
    }

    public ConnectorHealth(DataMap data) {
        super(data, SCHEMA);
    }

    public static ConnectorHealth.Fields fields() {
        return _fields;
    }

    /**
     * Existence checker for connectorType
     * 
     * @see Fields#connectorType
     */
    public boolean hasConnectorType() {
        return contains(FIELD_ConnectorType);
    }

    /**
     * Remover for connectorType
     * 
     * @see Fields#connectorType
     */
    public void removeConnectorType() {
        remove(FIELD_ConnectorType);
    }

    /**
     * Getter for connectorType
     * 
     * @see Fields#connectorType
     */
    public String getConnectorType(GetMode mode) {
        return obtainDirect(FIELD_ConnectorType, String.class, mode);
    }

    /**
     * Getter for connectorType
     * 
     * @see Fields#connectorType
     */
    public String getConnectorType() {
        return getConnectorType(GetMode.STRICT);
    }

    /**
     * Setter for connectorType
     * 
     * @see Fields#connectorType
     */
    public ConnectorHealth setConnectorType(String value, SetMode mode) {
        putDirect(FIELD_ConnectorType, String.class, String.class, value, mode);
        return this;
    }

    /**
     * Setter for connectorType
     * 
     * @see Fields#connectorType
     */
    public ConnectorHealth setConnectorType(String value) {
        putDirect(FIELD_ConnectorType, String.class, String.class, value, SetMode.DISALLOW_NULL);
        return this;
    }

    /**
     * Existence checker for strategy
     * 
     * @see Fields#strategy
     */
    public boolean hasStrategy() {
        return contains(FIELD_Strategy);
    }

    /**
     * Remover for strategy
     * 
     * @see Fields#strategy
     */
    public void removeStrategy() {
        remove(FIELD_Strategy);
    }

    /**
     * Getter for strategy
     * 
     * @see Fields#strategy
     */
    public String getStrategy(GetMode mode) {
        return obtainDirect(FIELD_Strategy, String.class, mode);
    }

    /**
     * Getter for strategy
     * 
     * @see Fields#strategy
     */
    public String getStrategy() {
        return getStrategy(GetMode.STRICT);
    }

    /**
     * Setter for strategy
     * 
     * @see Fields#strategy
     */
    public ConnectorHealth setStrategy(String value, SetMode mode) {
        putDirect(FIELD_Strategy, String.class, String.class, value, mode);
        return this;
    }

    /**
     * Setter for strategy
     * 
     * @see Fields#strategy
     */
    public ConnectorHealth setStrategy(String value) {
        putDirect(FIELD_Strategy, String.class, String.class, value, SetMode.DISALLOW_NULL);
        return this;
    }

    /**
     * Existence checker for tasks
     * 
     * @see Fields#tasks
     */
    public boolean hasTasks() {
        return contains(FIELD_Tasks);
    }

    /**
     * Remover for tasks
     * 
     * @see Fields#tasks
     */
    public void removeTasks() {
        remove(FIELD_Tasks);
    }

    /**
     * Getter for tasks
     * 
     * @see Fields#tasks
     */
    public TaskHealthArray getTasks(GetMode mode) {
        return obtainWrapped(FIELD_Tasks, TaskHealthArray.class, mode);
    }

    /**
     * Getter for tasks
     * 
     * @see Fields#tasks
     */
    public TaskHealthArray getTasks() {
        return getTasks(GetMode.STRICT);
    }

    /**
     * Setter for tasks
     * 
     * @see Fields#tasks
     */
    public ConnectorHealth setTasks(TaskHealthArray value, SetMode mode) {
        putWrapped(FIELD_Tasks, TaskHealthArray.class, value, mode);
        return this;
    }

    /**
     * Setter for tasks
     * 
     * @see Fields#tasks
     */
    public ConnectorHealth setTasks(TaskHealthArray value) {
        putWrapped(FIELD_Tasks, TaskHealthArray.class, value, SetMode.DISALLOW_NULL);
        return this;
    }

    @Override
    public ConnectorHealth clone()
        throws CloneNotSupportedException
    {
        return ((ConnectorHealth) super.clone());
    }

    @Override
    public ConnectorHealth copy()
        throws CloneNotSupportedException
    {
        return ((ConnectorHealth) super.copy());
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
         * type of the connector.
         * 
         */
        public PathSpec connectorType() {
            return new PathSpec(getPathComponents(), "connectorType");
        }

        /**
         * Strategy used by the connector.
         * 
         */
        public PathSpec strategy() {
            return new PathSpec(getPathComponents(), "strategy");
        }

        /**
         * tasks assigned to the connector Instance.
         * 
         */
        public com.linkedin.datastream.diagnostics.TaskHealthArray.Fields tasks() {
            return new com.linkedin.datastream.diagnostics.TaskHealthArray.Fields(getPathComponents(), "tasks");
        }

    }

}
