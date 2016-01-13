
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
 * Datastream task health
 * 
 */
@Generated(value = "com.linkedin.pegasus.generator.PegasusDataTemplateGenerator", comments = "LinkedIn Data Template. Generated from /Users/pdu/datastream/Datastream-github-new/datastream-common/src/main/pegasus/com/linkedin/datastream/diagnostics/TaskHealth.pdsc.", date = "Wed Jan 13 10:40:34 PST 2016")
public class TaskHealth
    extends RecordTemplate
{

    private final static TaskHealth.Fields _fields = new TaskHealth.Fields();
    private final static RecordDataSchema SCHEMA = ((RecordDataSchema) DataTemplateUtil.parseSchema("{\"type\":\"record\",\"name\":\"TaskHealth\",\"namespace\":\"com.linkedin.datastream.diagnostics\",\"doc\":\"Datastream task health\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name of the task.\"},{\"name\":\"datastreams\",\"type\":\"string\",\"doc\":\"Name of the datastreams associated with the task.\"},{\"name\":\"partitions\",\"type\":\"string\",\"doc\":\"Partitions associated with the task.\"},{\"name\":\"source\",\"type\":\"string\",\"doc\":\"Source of the datastream.\"},{\"name\":\"destination\",\"type\":\"string\",\"doc\":\"Destination of the datastream.\"},{\"name\":\"statusCode\",\"type\":\"string\",\"doc\":\"Status code of the datastream task.\"},{\"name\":\"statusMessage\",\"type\":\"string\",\"doc\":\"Status message of the datastream task.\"},{\"name\":\"sourceCheckpoint\",\"type\":\"string\",\"doc\":\"Source checkpoint.\"}]}"));
    private final static RecordDataSchema.Field FIELD_Name = SCHEMA.getField("name");
    private final static RecordDataSchema.Field FIELD_Datastreams = SCHEMA.getField("datastreams");
    private final static RecordDataSchema.Field FIELD_Partitions = SCHEMA.getField("partitions");
    private final static RecordDataSchema.Field FIELD_Source = SCHEMA.getField("source");
    private final static RecordDataSchema.Field FIELD_Destination = SCHEMA.getField("destination");
    private final static RecordDataSchema.Field FIELD_StatusCode = SCHEMA.getField("statusCode");
    private final static RecordDataSchema.Field FIELD_StatusMessage = SCHEMA.getField("statusMessage");
    private final static RecordDataSchema.Field FIELD_SourceCheckpoint = SCHEMA.getField("sourceCheckpoint");

    public TaskHealth() {
        super(new DataMap(), SCHEMA);
    }

    public TaskHealth(DataMap data) {
        super(data, SCHEMA);
    }

    public static TaskHealth.Fields fields() {
        return _fields;
    }

    /**
     * Existence checker for name
     * 
     * @see Fields#name
     */
    public boolean hasName() {
        return contains(FIELD_Name);
    }

    /**
     * Remover for name
     * 
     * @see Fields#name
     */
    public void removeName() {
        remove(FIELD_Name);
    }

    /**
     * Getter for name
     * 
     * @see Fields#name
     */
    public String getName(GetMode mode) {
        return obtainDirect(FIELD_Name, String.class, mode);
    }

    /**
     * Getter for name
     * 
     * @see Fields#name
     */
    public String getName() {
        return getName(GetMode.STRICT);
    }

    /**
     * Setter for name
     * 
     * @see Fields#name
     */
    public TaskHealth setName(String value, SetMode mode) {
        putDirect(FIELD_Name, String.class, String.class, value, mode);
        return this;
    }

    /**
     * Setter for name
     * 
     * @see Fields#name
     */
    public TaskHealth setName(String value) {
        putDirect(FIELD_Name, String.class, String.class, value, SetMode.DISALLOW_NULL);
        return this;
    }

    /**
     * Existence checker for datastreams
     * 
     * @see Fields#datastreams
     */
    public boolean hasDatastreams() {
        return contains(FIELD_Datastreams);
    }

    /**
     * Remover for datastreams
     * 
     * @see Fields#datastreams
     */
    public void removeDatastreams() {
        remove(FIELD_Datastreams);
    }

    /**
     * Getter for datastreams
     * 
     * @see Fields#datastreams
     */
    public String getDatastreams(GetMode mode) {
        return obtainDirect(FIELD_Datastreams, String.class, mode);
    }

    /**
     * Getter for datastreams
     * 
     * @see Fields#datastreams
     */
    public String getDatastreams() {
        return getDatastreams(GetMode.STRICT);
    }

    /**
     * Setter for datastreams
     * 
     * @see Fields#datastreams
     */
    public TaskHealth setDatastreams(String value, SetMode mode) {
        putDirect(FIELD_Datastreams, String.class, String.class, value, mode);
        return this;
    }

    /**
     * Setter for datastreams
     * 
     * @see Fields#datastreams
     */
    public TaskHealth setDatastreams(String value) {
        putDirect(FIELD_Datastreams, String.class, String.class, value, SetMode.DISALLOW_NULL);
        return this;
    }

    /**
     * Existence checker for partitions
     * 
     * @see Fields#partitions
     */
    public boolean hasPartitions() {
        return contains(FIELD_Partitions);
    }

    /**
     * Remover for partitions
     * 
     * @see Fields#partitions
     */
    public void removePartitions() {
        remove(FIELD_Partitions);
    }

    /**
     * Getter for partitions
     * 
     * @see Fields#partitions
     */
    public String getPartitions(GetMode mode) {
        return obtainDirect(FIELD_Partitions, String.class, mode);
    }

    /**
     * Getter for partitions
     * 
     * @see Fields#partitions
     */
    public String getPartitions() {
        return getPartitions(GetMode.STRICT);
    }

    /**
     * Setter for partitions
     * 
     * @see Fields#partitions
     */
    public TaskHealth setPartitions(String value, SetMode mode) {
        putDirect(FIELD_Partitions, String.class, String.class, value, mode);
        return this;
    }

    /**
     * Setter for partitions
     * 
     * @see Fields#partitions
     */
    public TaskHealth setPartitions(String value) {
        putDirect(FIELD_Partitions, String.class, String.class, value, SetMode.DISALLOW_NULL);
        return this;
    }

    /**
     * Existence checker for source
     * 
     * @see Fields#source
     */
    public boolean hasSource() {
        return contains(FIELD_Source);
    }

    /**
     * Remover for source
     * 
     * @see Fields#source
     */
    public void removeSource() {
        remove(FIELD_Source);
    }

    /**
     * Getter for source
     * 
     * @see Fields#source
     */
    public String getSource(GetMode mode) {
        return obtainDirect(FIELD_Source, String.class, mode);
    }

    /**
     * Getter for source
     * 
     * @see Fields#source
     */
    public String getSource() {
        return getSource(GetMode.STRICT);
    }

    /**
     * Setter for source
     * 
     * @see Fields#source
     */
    public TaskHealth setSource(String value, SetMode mode) {
        putDirect(FIELD_Source, String.class, String.class, value, mode);
        return this;
    }

    /**
     * Setter for source
     * 
     * @see Fields#source
     */
    public TaskHealth setSource(String value) {
        putDirect(FIELD_Source, String.class, String.class, value, SetMode.DISALLOW_NULL);
        return this;
    }

    /**
     * Existence checker for destination
     * 
     * @see Fields#destination
     */
    public boolean hasDestination() {
        return contains(FIELD_Destination);
    }

    /**
     * Remover for destination
     * 
     * @see Fields#destination
     */
    public void removeDestination() {
        remove(FIELD_Destination);
    }

    /**
     * Getter for destination
     * 
     * @see Fields#destination
     */
    public String getDestination(GetMode mode) {
        return obtainDirect(FIELD_Destination, String.class, mode);
    }

    /**
     * Getter for destination
     * 
     * @see Fields#destination
     */
    public String getDestination() {
        return getDestination(GetMode.STRICT);
    }

    /**
     * Setter for destination
     * 
     * @see Fields#destination
     */
    public TaskHealth setDestination(String value, SetMode mode) {
        putDirect(FIELD_Destination, String.class, String.class, value, mode);
        return this;
    }

    /**
     * Setter for destination
     * 
     * @see Fields#destination
     */
    public TaskHealth setDestination(String value) {
        putDirect(FIELD_Destination, String.class, String.class, value, SetMode.DISALLOW_NULL);
        return this;
    }

    /**
     * Existence checker for statusCode
     * 
     * @see Fields#statusCode
     */
    public boolean hasStatusCode() {
        return contains(FIELD_StatusCode);
    }

    /**
     * Remover for statusCode
     * 
     * @see Fields#statusCode
     */
    public void removeStatusCode() {
        remove(FIELD_StatusCode);
    }

    /**
     * Getter for statusCode
     * 
     * @see Fields#statusCode
     */
    public String getStatusCode(GetMode mode) {
        return obtainDirect(FIELD_StatusCode, String.class, mode);
    }

    /**
     * Getter for statusCode
     * 
     * @see Fields#statusCode
     */
    public String getStatusCode() {
        return getStatusCode(GetMode.STRICT);
    }

    /**
     * Setter for statusCode
     * 
     * @see Fields#statusCode
     */
    public TaskHealth setStatusCode(String value, SetMode mode) {
        putDirect(FIELD_StatusCode, String.class, String.class, value, mode);
        return this;
    }

    /**
     * Setter for statusCode
     * 
     * @see Fields#statusCode
     */
    public TaskHealth setStatusCode(String value) {
        putDirect(FIELD_StatusCode, String.class, String.class, value, SetMode.DISALLOW_NULL);
        return this;
    }

    /**
     * Existence checker for statusMessage
     * 
     * @see Fields#statusMessage
     */
    public boolean hasStatusMessage() {
        return contains(FIELD_StatusMessage);
    }

    /**
     * Remover for statusMessage
     * 
     * @see Fields#statusMessage
     */
    public void removeStatusMessage() {
        remove(FIELD_StatusMessage);
    }

    /**
     * Getter for statusMessage
     * 
     * @see Fields#statusMessage
     */
    public String getStatusMessage(GetMode mode) {
        return obtainDirect(FIELD_StatusMessage, String.class, mode);
    }

    /**
     * Getter for statusMessage
     * 
     * @see Fields#statusMessage
     */
    public String getStatusMessage() {
        return getStatusMessage(GetMode.STRICT);
    }

    /**
     * Setter for statusMessage
     * 
     * @see Fields#statusMessage
     */
    public TaskHealth setStatusMessage(String value, SetMode mode) {
        putDirect(FIELD_StatusMessage, String.class, String.class, value, mode);
        return this;
    }

    /**
     * Setter for statusMessage
     * 
     * @see Fields#statusMessage
     */
    public TaskHealth setStatusMessage(String value) {
        putDirect(FIELD_StatusMessage, String.class, String.class, value, SetMode.DISALLOW_NULL);
        return this;
    }

    /**
     * Existence checker for sourceCheckpoint
     * 
     * @see Fields#sourceCheckpoint
     */
    public boolean hasSourceCheckpoint() {
        return contains(FIELD_SourceCheckpoint);
    }

    /**
     * Remover for sourceCheckpoint
     * 
     * @see Fields#sourceCheckpoint
     */
    public void removeSourceCheckpoint() {
        remove(FIELD_SourceCheckpoint);
    }

    /**
     * Getter for sourceCheckpoint
     * 
     * @see Fields#sourceCheckpoint
     */
    public String getSourceCheckpoint(GetMode mode) {
        return obtainDirect(FIELD_SourceCheckpoint, String.class, mode);
    }

    /**
     * Getter for sourceCheckpoint
     * 
     * @see Fields#sourceCheckpoint
     */
    public String getSourceCheckpoint() {
        return getSourceCheckpoint(GetMode.STRICT);
    }

    /**
     * Setter for sourceCheckpoint
     * 
     * @see Fields#sourceCheckpoint
     */
    public TaskHealth setSourceCheckpoint(String value, SetMode mode) {
        putDirect(FIELD_SourceCheckpoint, String.class, String.class, value, mode);
        return this;
    }

    /**
     * Setter for sourceCheckpoint
     * 
     * @see Fields#sourceCheckpoint
     */
    public TaskHealth setSourceCheckpoint(String value) {
        putDirect(FIELD_SourceCheckpoint, String.class, String.class, value, SetMode.DISALLOW_NULL);
        return this;
    }

    @Override
    public TaskHealth clone()
        throws CloneNotSupportedException
    {
        return ((TaskHealth) super.clone());
    }

    @Override
    public TaskHealth copy()
        throws CloneNotSupportedException
    {
        return ((TaskHealth) super.copy());
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
         * name of the task.
         * 
         */
        public PathSpec name() {
            return new PathSpec(getPathComponents(), "name");
        }

        /**
         * Name of the datastreams associated with the task.
         * 
         */
        public PathSpec datastreams() {
            return new PathSpec(getPathComponents(), "datastreams");
        }

        /**
         * Partitions associated with the task.
         * 
         */
        public PathSpec partitions() {
            return new PathSpec(getPathComponents(), "partitions");
        }

        /**
         * Source of the datastream.
         * 
         */
        public PathSpec source() {
            return new PathSpec(getPathComponents(), "source");
        }

        /**
         * Destination of the datastream.
         * 
         */
        public PathSpec destination() {
            return new PathSpec(getPathComponents(), "destination");
        }

        /**
         * Status code of the datastream task.
         * 
         */
        public PathSpec statusCode() {
            return new PathSpec(getPathComponents(), "statusCode");
        }

        /**
         * Status message of the datastream task.
         * 
         */
        public PathSpec statusMessage() {
            return new PathSpec(getPathComponents(), "statusMessage");
        }

        /**
         * Source checkpoint.
         * 
         */
        public PathSpec sourceCheckpoint() {
            return new PathSpec(getPathComponents(), "sourceCheckpoint");
        }

    }

}
