
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
import com.linkedin.data.template.StringMap;


/**
 * Extensible data model of Datastream
 * 
 */
@Generated(value = "com.linkedin.pegasus.generator.PegasusDataTemplateGenerator", comments = "LinkedIn Data Template. Generated from /Users/spunuru/datastream/ds/datastream-common/src/main/pegasus/com/linkedin/datastream/common/Datastream.pdsc.", date = "Wed Dec 09 10:45:32 PST 2015")
public class Datastream
    extends RecordTemplate
{

    private final static Datastream.Fields _fields = new Datastream.Fields();
    private final static RecordDataSchema SCHEMA = ((RecordDataSchema) DataTemplateUtil.parseSchema("{\"type\":\"record\",\"name\":\"Datastream\",\"namespace\":\"com.linkedin.datastream.common\",\"doc\":\"Extensible data model of Datastream\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"Name of the Datastream.\"},{\"name\":\"connectorType\",\"type\":\"string\",\"doc\":\"Type of the connector to be used for reading the change capture events from the source, e.g. Oracle-Change, Espresso-Change, Oracle-Bootstrap, Espresso-Bootstrap, Mysql-Change etc..\"},{\"name\":\"source\",\"type\":{\"type\":\"record\",\"name\":\"DatastreamSource\",\"doc\":\"Datastream source that connector will use to consume events\",\"fields\":[{\"name\":\"connectionString\",\"type\":\"string\",\"doc\":\"Source connection string to consume the data from.\"}]},\"doc\":\"Source that connector can use to connect to the data store and consume the data.\"},{\"name\":\"destination\",\"type\":{\"type\":\"record\",\"name\":\"DatastreamDestination\",\"doc\":\"Datastream destination details that the transport provider will use to send events\",\"fields\":[{\"name\":\"connectionString\",\"type\":\"string\",\"doc\":\"Source connection string to consume the data from.\"},{\"name\":\"partitions\",\"type\":\"int\",\"doc\":\"Number of partitions of the kafka topic.\"}]},\"doc\":\"Datastream destination string that the transport provider will use to send the events\",\"optional\":true},{\"name\":\"metadata\",\"type\":{\"type\":\"map\",\"values\":\"string\"},\"doc\":\"Generic metadata for Datastream (e.g. owner, expiration, etc). Metadatas are stored as user defined name/value pair.\",\"optional\":true}]}"));
    private final static RecordDataSchema.Field FIELD_Name = SCHEMA.getField("name");
    private final static RecordDataSchema.Field FIELD_ConnectorType = SCHEMA.getField("connectorType");
    private final static RecordDataSchema.Field FIELD_Source = SCHEMA.getField("source");
    private final static RecordDataSchema.Field FIELD_Destination = SCHEMA.getField("destination");
    private final static RecordDataSchema.Field FIELD_Metadata = SCHEMA.getField("metadata");

    public Datastream() {
        super(new DataMap(), SCHEMA);
    }

    public Datastream(DataMap data) {
        super(data, SCHEMA);
    }

    public static Datastream.Fields fields() {
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
    public Datastream setName(String value, SetMode mode) {
        putDirect(FIELD_Name, String.class, String.class, value, mode);
        return this;
    }

    /**
     * Setter for name
     * 
     * @see Fields#name
     */
    public Datastream setName(String value) {
        putDirect(FIELD_Name, String.class, String.class, value, SetMode.DISALLOW_NULL);
        return this;
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
    public Datastream setConnectorType(String value, SetMode mode) {
        putDirect(FIELD_ConnectorType, String.class, String.class, value, mode);
        return this;
    }

    /**
     * Setter for connectorType
     * 
     * @see Fields#connectorType
     */
    public Datastream setConnectorType(String value) {
        putDirect(FIELD_ConnectorType, String.class, String.class, value, SetMode.DISALLOW_NULL);
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
    public DatastreamSource getSource(GetMode mode) {
        return obtainWrapped(FIELD_Source, DatastreamSource.class, mode);
    }

    /**
     * Getter for source
     * 
     * @see Fields#source
     */
    public DatastreamSource getSource() {
        return getSource(GetMode.STRICT);
    }

    /**
     * Setter for source
     * 
     * @see Fields#source
     */
    public Datastream setSource(DatastreamSource value, SetMode mode) {
        putWrapped(FIELD_Source, DatastreamSource.class, value, mode);
        return this;
    }

    /**
     * Setter for source
     * 
     * @see Fields#source
     */
    public Datastream setSource(DatastreamSource value) {
        putWrapped(FIELD_Source, DatastreamSource.class, value, SetMode.DISALLOW_NULL);
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
    public DatastreamDestination getDestination(GetMode mode) {
        return obtainWrapped(FIELD_Destination, DatastreamDestination.class, mode);
    }

    /**
     * Getter for destination
     * 
     * @see Fields#destination
     */
    public DatastreamDestination getDestination() {
        return getDestination(GetMode.STRICT);
    }

    /**
     * Setter for destination
     * 
     * @see Fields#destination
     */
    public Datastream setDestination(DatastreamDestination value, SetMode mode) {
        putWrapped(FIELD_Destination, DatastreamDestination.class, value, mode);
        return this;
    }

    /**
     * Setter for destination
     * 
     * @see Fields#destination
     */
    public Datastream setDestination(DatastreamDestination value) {
        putWrapped(FIELD_Destination, DatastreamDestination.class, value, SetMode.DISALLOW_NULL);
        return this;
    }

    /**
     * Existence checker for metadata
     * 
     * @see Fields#metadata
     */
    public boolean hasMetadata() {
        return contains(FIELD_Metadata);
    }

    /**
     * Remover for metadata
     * 
     * @see Fields#metadata
     */
    public void removeMetadata() {
        remove(FIELD_Metadata);
    }

    /**
     * Getter for metadata
     * 
     * @see Fields#metadata
     */
    public StringMap getMetadata(GetMode mode) {
        return obtainWrapped(FIELD_Metadata, StringMap.class, mode);
    }

    /**
     * Getter for metadata
     * 
     * @see Fields#metadata
     */
    public StringMap getMetadata() {
        return getMetadata(GetMode.STRICT);
    }

    /**
     * Setter for metadata
     * 
     * @see Fields#metadata
     */
    public Datastream setMetadata(StringMap value, SetMode mode) {
        putWrapped(FIELD_Metadata, StringMap.class, value, mode);
        return this;
    }

    /**
     * Setter for metadata
     * 
     * @see Fields#metadata
     */
    public Datastream setMetadata(StringMap value) {
        putWrapped(FIELD_Metadata, StringMap.class, value, SetMode.DISALLOW_NULL);
        return this;
    }

    @Override
    public Datastream clone()
        throws CloneNotSupportedException
    {
        return ((Datastream) super.clone());
    }

    @Override
    public Datastream copy()
        throws CloneNotSupportedException
    {
        return ((Datastream) super.copy());
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
         * Name of the Datastream.
         * 
         */
        public PathSpec name() {
            return new PathSpec(getPathComponents(), "name");
        }

        /**
         * Type of the connector to be used for reading the change capture events from the source, e.g. Oracle-Change, Espresso-Change, Oracle-Bootstrap, Espresso-Bootstrap, Mysql-Change etc..
         * 
         */
        public PathSpec connectorType() {
            return new PathSpec(getPathComponents(), "connectorType");
        }

        /**
         * Source that connector can use to connect to the data store and consume the data.
         * 
         */
        public com.linkedin.datastream.common.DatastreamSource.Fields source() {
            return new com.linkedin.datastream.common.DatastreamSource.Fields(getPathComponents(), "source");
        }

        /**
         * Datastream destination string that the transport provider will use to send the events
         * 
         */
        public com.linkedin.datastream.common.DatastreamDestination.Fields destination() {
            return new com.linkedin.datastream.common.DatastreamDestination.Fields(getPathComponents(), "destination");
        }

        /**
         * Generic metadata for Datastream (e.g. owner, expiration, etc). Metadatas are stored as user defined name/value pair.
         * 
         */
        public PathSpec metadata() {
            return new PathSpec(getPathComponents(), "metadata");
        }

    }

}
