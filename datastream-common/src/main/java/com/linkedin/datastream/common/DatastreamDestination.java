
package com.linkedin.datastream.common;

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
 * Datastream destination details that the transport provider will use to send events
 * 
 */
@Generated(value = "com.linkedin.pegasus.generator.PegasusDataTemplateGenerator", comments = "LinkedIn Data Template. Generated from /Users/spunuru/datastream/ds/datastream-common/src/main/pegasus/com/linkedin/datastream/common/Datastream.pdsc.", date = "Wed Dec 09 10:45:32 PST 2015")
public class DatastreamDestination
    extends RecordTemplate
{

    private final static DatastreamDestination.Fields _fields = new DatastreamDestination.Fields();
    private final static RecordDataSchema SCHEMA = ((RecordDataSchema) DataTemplateUtil.parseSchema("{\"type\":\"record\",\"name\":\"DatastreamDestination\",\"namespace\":\"com.linkedin.datastream.common\",\"doc\":\"Datastream destination details that the transport provider will use to send events\",\"fields\":[{\"name\":\"connectionString\",\"type\":\"string\",\"doc\":\"Source connection string to consume the data from.\"},{\"name\":\"partitions\",\"type\":\"int\",\"doc\":\"Number of partitions of the kafka topic.\"}]}"));
    private final static RecordDataSchema.Field FIELD_ConnectionString = SCHEMA.getField("connectionString");
    private final static RecordDataSchema.Field FIELD_Partitions = SCHEMA.getField("partitions");

    public DatastreamDestination() {
        super(new DataMap(), SCHEMA);
    }

    public DatastreamDestination(DataMap data) {
        super(data, SCHEMA);
    }

    public static DatastreamDestination.Fields fields() {
        return _fields;
    }

    /**
     * Existence checker for connectionString
     * 
     * @see Fields#connectionString
     */
    public boolean hasConnectionString() {
        return contains(FIELD_ConnectionString);
    }

    /**
     * Remover for connectionString
     * 
     * @see Fields#connectionString
     */
    public void removeConnectionString() {
        remove(FIELD_ConnectionString);
    }

    /**
     * Getter for connectionString
     * 
     * @see Fields#connectionString
     */
    public String getConnectionString(GetMode mode) {
        return obtainDirect(FIELD_ConnectionString, String.class, mode);
    }

    /**
     * Getter for connectionString
     * 
     * @see Fields#connectionString
     */
    public String getConnectionString() {
        return getConnectionString(GetMode.STRICT);
    }

    /**
     * Setter for connectionString
     * 
     * @see Fields#connectionString
     */
    public DatastreamDestination setConnectionString(String value, SetMode mode) {
        putDirect(FIELD_ConnectionString, String.class, String.class, value, mode);
        return this;
    }

    /**
     * Setter for connectionString
     * 
     * @see Fields#connectionString
     */
    public DatastreamDestination setConnectionString(String value) {
        putDirect(FIELD_ConnectionString, String.class, String.class, value, SetMode.DISALLOW_NULL);
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
    public Integer getPartitions(GetMode mode) {
        return obtainDirect(FIELD_Partitions, Integer.class, mode);
    }

    /**
     * Getter for partitions
     * 
     * @see Fields#partitions
     */
    public Integer getPartitions() {
        return getPartitions(GetMode.STRICT);
    }

    /**
     * Setter for partitions
     * 
     * @see Fields#partitions
     */
    public DatastreamDestination setPartitions(Integer value, SetMode mode) {
        putDirect(FIELD_Partitions, Integer.class, Integer.class, value, mode);
        return this;
    }

    /**
     * Setter for partitions
     * 
     * @see Fields#partitions
     */
    public DatastreamDestination setPartitions(Integer value) {
        putDirect(FIELD_Partitions, Integer.class, Integer.class, value, SetMode.DISALLOW_NULL);
        return this;
    }

    /**
     * Setter for partitions
     * 
     * @see Fields#partitions
     */
//    public DatastreamDestination setPartitions(int value) {
//        putDirect(FIELD_Partitions, Integer.class, Integer.class, value, SetMode.DISALLOW_NULL);
//        return this;
//    }

    @Override
    public DatastreamDestination clone()
        throws CloneNotSupportedException
    {
        return ((DatastreamDestination) super.clone());
    }

    @Override
    public DatastreamDestination copy()
        throws CloneNotSupportedException
    {
        return ((DatastreamDestination) super.copy());
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
         * Source connection string to consume the data from.
         * 
         */
        public PathSpec connectionString() {
            return new PathSpec(getPathComponents(), "connectionString");
        }

        /**
         * Number of partitions of the kafka topic.
         * 
         */
        public PathSpec partitions() {
            return new PathSpec(getPathComponents(), "partitions");
        }

    }

}
