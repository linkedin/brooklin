
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
 * Extensible data model of Datastream
 * 
 */
@Generated(value = "com.linkedin.pegasus.generator.PegasusDataTemplateGenerator", comments = "LinkedIn Data Template. Generated from /Users/halu/work/datastream/datastream-common/src/main/pegasus/com/linkedin/datastream/common/Datastream.pdsc.", date = "Tue Aug 18 23:33:16 PDT 2015")
public class Datastream
    extends RecordTemplate
{

    private final static Datastream.Fields _fields = new Datastream.Fields();
    private final static RecordDataSchema SCHEMA = ((RecordDataSchema) DataTemplateUtil.parseSchema("{\"type\":\"record\",\"name\":\"Datastream\",\"namespace\":\"com.linkedin.datastream.common\",\"doc\":\"Extensible data model of Datastream\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"This is the name of the Datastream.\"},{\"name\":\"connectorType\",\"type\":\"string\",\"doc\":\"Type of the connector to be used for reading the real time change capture events from the source, e.g. Oracle, Espresso, Mysql etc..\"},{\"name\":\"connectionString\",\"type\":\"string\",\"doc\":\"Connection string that connector can use to connect to the database.\"},{\"name\":\"topicName\",\"type\":\"string\",\"doc\":\"Name of the topic to be used for writing the change capture events. Populated by connector\",\"optional\":true},{\"name\":\"kafkaMetadataBrokers\",\"type\":\"string\",\"doc\":\"Comma separated list of kafka metadata brokers in which the output topic needs to be written. Populated by connector\",\"optional\":true},{\"name\":\"owner\",\"type\":\"string\",\"doc\":\"Owner of the Datastream.\",\"optional\":true}]}"));
    private final static RecordDataSchema.Field FIELD_Name = SCHEMA.getField("name");
    private final static RecordDataSchema.Field FIELD_ConnectorType = SCHEMA.getField("connectorType");
    private final static RecordDataSchema.Field FIELD_ConnectionString = SCHEMA.getField("connectionString");
    private final static RecordDataSchema.Field FIELD_TopicName = SCHEMA.getField("topicName");
    private final static RecordDataSchema.Field FIELD_KafkaMetadataBrokers = SCHEMA.getField("kafkaMetadataBrokers");
    private final static RecordDataSchema.Field FIELD_Owner = SCHEMA.getField("owner");

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
    public Datastream setConnectionString(String value, SetMode mode) {
        putDirect(FIELD_ConnectionString, String.class, String.class, value, mode);
        return this;
    }

    /**
     * Setter for connectionString
     * 
     * @see Fields#connectionString
     */
    public Datastream setConnectionString(String value) {
        putDirect(FIELD_ConnectionString, String.class, String.class, value, SetMode.DISALLOW_NULL);
        return this;
    }

    /**
     * Existence checker for topicName
     * 
     * @see Fields#topicName
     */
    public boolean hasTopicName() {
        return contains(FIELD_TopicName);
    }

    /**
     * Remover for topicName
     * 
     * @see Fields#topicName
     */
    public void removeTopicName() {
        remove(FIELD_TopicName);
    }

    /**
     * Getter for topicName
     * 
     * @see Fields#topicName
     */
    public String getTopicName(GetMode mode) {
        return obtainDirect(FIELD_TopicName, String.class, mode);
    }

    /**
     * Getter for topicName
     * 
     * @see Fields#topicName
     */
    public String getTopicName() {
        return getTopicName(GetMode.STRICT);
    }

    /**
     * Setter for topicName
     * 
     * @see Fields#topicName
     */
    public Datastream setTopicName(String value, SetMode mode) {
        putDirect(FIELD_TopicName, String.class, String.class, value, mode);
        return this;
    }

    /**
     * Setter for topicName
     * 
     * @see Fields#topicName
     */
    public Datastream setTopicName(String value) {
        putDirect(FIELD_TopicName, String.class, String.class, value, SetMode.DISALLOW_NULL);
        return this;
    }

    /**
     * Existence checker for kafkaMetadataBrokers
     * 
     * @see Fields#kafkaMetadataBrokers
     */
    public boolean hasKafkaMetadataBrokers() {
        return contains(FIELD_KafkaMetadataBrokers);
    }

    /**
     * Remover for kafkaMetadataBrokers
     * 
     * @see Fields#kafkaMetadataBrokers
     */
    public void removeKafkaMetadataBrokers() {
        remove(FIELD_KafkaMetadataBrokers);
    }

    /**
     * Getter for kafkaMetadataBrokers
     * 
     * @see Fields#kafkaMetadataBrokers
     */
    public String getKafkaMetadataBrokers(GetMode mode) {
        return obtainDirect(FIELD_KafkaMetadataBrokers, String.class, mode);
    }

    /**
     * Getter for kafkaMetadataBrokers
     * 
     * @see Fields#kafkaMetadataBrokers
     */
    public String getKafkaMetadataBrokers() {
        return getKafkaMetadataBrokers(GetMode.STRICT);
    }

    /**
     * Setter for kafkaMetadataBrokers
     * 
     * @see Fields#kafkaMetadataBrokers
     */
    public Datastream setKafkaMetadataBrokers(String value, SetMode mode) {
        putDirect(FIELD_KafkaMetadataBrokers, String.class, String.class, value, mode);
        return this;
    }

    /**
     * Setter for kafkaMetadataBrokers
     * 
     * @see Fields#kafkaMetadataBrokers
     */
    public Datastream setKafkaMetadataBrokers(String value) {
        putDirect(FIELD_KafkaMetadataBrokers, String.class, String.class, value, SetMode.DISALLOW_NULL);
        return this;
    }

    /**
     * Existence checker for owner
     * 
     * @see Fields#owner
     */
    public boolean hasOwner() {
        return contains(FIELD_Owner);
    }

    /**
     * Remover for owner
     * 
     * @see Fields#owner
     */
    public void removeOwner() {
        remove(FIELD_Owner);
    }

    /**
     * Getter for owner
     * 
     * @see Fields#owner
     */
    public String getOwner(GetMode mode) {
        return obtainDirect(FIELD_Owner, String.class, mode);
    }

    /**
     * Getter for owner
     * 
     * @see Fields#owner
     */
    public String getOwner() {
        return getOwner(GetMode.STRICT);
    }

    /**
     * Setter for owner
     * 
     * @see Fields#owner
     */
    public Datastream setOwner(String value, SetMode mode) {
        putDirect(FIELD_Owner, String.class, String.class, value, mode);
        return this;
    }

    /**
     * Setter for owner
     * 
     * @see Fields#owner
     */
    public Datastream setOwner(String value) {
        putDirect(FIELD_Owner, String.class, String.class, value, SetMode.DISALLOW_NULL);
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
         * This is the name of the Datastream.
         * 
         */
        public PathSpec name() {
            return new PathSpec(getPathComponents(), "name");
        }

        /**
         * Type of the connector to be used for reading the real time change capture events from the source, e.g. Oracle, Espresso, Mysql etc..
         * 
         */
        public PathSpec connectorType() {
            return new PathSpec(getPathComponents(), "connectorType");
        }

        /**
         * Connection string that connector can use to connect to the database.
         * 
         */
        public PathSpec connectionString() {
            return new PathSpec(getPathComponents(), "connectionString");
        }

        /**
         * Name of the topic to be used for writing the change capture events. Populated by connector
         * 
         */
        public PathSpec topicName() {
            return new PathSpec(getPathComponents(), "topicName");
        }

        /**
         * Comma separated list of kafka metadata brokers in which the output topic needs to be written. Populated by connector
         * 
         */
        public PathSpec kafkaMetadataBrokers() {
            return new PathSpec(getPathComponents(), "kafkaMetadataBrokers");
        }

        /**
         * Owner of the Datastream.
         * 
         */
        public PathSpec owner() {
            return new PathSpec(getPathComponents(), "owner");
        }

    }

}
