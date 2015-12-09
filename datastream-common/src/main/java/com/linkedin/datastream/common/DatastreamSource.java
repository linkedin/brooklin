
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
@Generated(value = "com.linkedin.pegasus.generator.PegasusDataTemplateGenerator", comments = "LinkedIn Data Template. Generated from /Users/spunuru/datastream/ds/datastream-common/src/main/pegasus/com/linkedin/datastream/common/Datastream.pdsc.", date = "Wed Dec 09 10:45:32 PST 2015")
public class DatastreamSource
    extends RecordTemplate
{

    private final static DatastreamSource.Fields _fields = new DatastreamSource.Fields();
    private final static RecordDataSchema SCHEMA = ((RecordDataSchema) DataTemplateUtil.parseSchema("{\"type\":\"record\",\"name\":\"DatastreamSource\",\"namespace\":\"com.linkedin.datastream.common\",\"doc\":\"Datastream source that connector will use to consume events\",\"fields\":[{\"name\":\"connectionString\",\"type\":\"string\",\"doc\":\"Source connection string to consume the data from.\"}]}"));
    private final static RecordDataSchema.Field FIELD_ConnectionString = SCHEMA.getField("connectionString");

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

    }

}
