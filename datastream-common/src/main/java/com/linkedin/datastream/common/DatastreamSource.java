
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
 * Datastream source that connector will use to consume events
 * 
 */
@Generated(value = "com.linkedin.pegasus.generator.PegasusDataTemplateGenerator", comments = "LinkedIn Data Template. Generated from /Users/spunuru/datastream/ds/datastream-common/src/main/pegasus/com/linkedin/datastream/common/Datastream.pdsc.", date = "Fri Jan 15 13:18:15 PST 2016")
public class DatastreamSource
    extends RecordTemplate
{

    private final static DatastreamSource.Fields _fields = new DatastreamSource.Fields();
    private final static RecordDataSchema SCHEMA = ((RecordDataSchema) DataTemplateUtil.parseSchema("{\"type\":\"record\",\"name\":\"DatastreamSource\",\"namespace\":\"com.linkedin.datastream.common\",\"doc\":\"Datastream source that connector will use to consume events\",\"fields\":[{\"name\":\"connectionString\",\"type\":\"string\",\"doc\":\"Source connection string to consume the data from.\"},{\"name\":\"partitions\",\"type\":\"int\",\"doc\":\"Number of partitions in the source.\"}]}"));
    private final static RecordDataSchema.Field FIELD_ConnectionString = SCHEMA.getField("connectionString");
    private final static RecordDataSchema.Field FIELD_Partitions = SCHEMA.getField("partitions");

    public DatastreamSource() {
        super(new DataMap(), SCHEMA);
    }

    public DatastreamSource(DataMap data) {
        super(data, SCHEMA);
    }

    public static DatastreamSource.Fields fields() {
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
    public DatastreamSource setConnectionString(String value, SetMode mode) {
        putDirect(FIELD_ConnectionString, String.class, String.class, value, mode);
        return this;
    }

    /**
     * Setter for connectionString
     * 
     * @see Fields#connectionString
     */
    public DatastreamSource setConnectionString(String value) {
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
    public DatastreamSource setPartitions(Integer value, SetMode mode) {
        putDirect(FIELD_Partitions, Integer.class, Integer.class, value, mode);
        return this;
    }

    /**
     * Setter for partitions
     * 
     * @see Fields#partitions
     */
//    public DatastreamSource setPartitions(Integer value) {
//        putDirect(FIELD_Partitions, Integer.class, Integer.class, value, SetMode.DISALLOW_NULL);
//        return this;
//    }

    /**
     * Setter for partitions
     * 
     * @see Fields#partitions
     */
    public DatastreamSource setPartitions(int value) {
        putDirect(FIELD_Partitions, Integer.class, Integer.class, value, SetMode.DISALLOW_NULL);
        return this;
    }

    @Override
    public DatastreamSource clone()
        throws CloneNotSupportedException
    {
        return ((DatastreamSource) super.clone());
    }

    @Override
    public DatastreamSource copy()
        throws CloneNotSupportedException
    {
        return ((DatastreamSource) super.copy());
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
         * Number of partitions in the source.
         * 
         */
        public PathSpec partitions() {
            return new PathSpec(getPathComponents(), "partitions");
        }

    }

}
