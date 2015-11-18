
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
 * 
 * 
 */
@Generated(value = "com.linkedin.pegasus.generator.PegasusDataTemplateGenerator", comments = "LinkedIn Data Template. Generated from /Users/spunuru/datastream/ds/datastream-common/src/main/pegasus/com/linkedin/datastream/common/Datastream.pdsc.", date = "Tue Nov 17 09:42:50 PST 2015")
public class KafkaConnection
    extends RecordTemplate
{

    private final static KafkaConnection.Fields _fields = new KafkaConnection.Fields();
    private final static RecordDataSchema SCHEMA = ((RecordDataSchema) DataTemplateUtil.parseSchema("{\"type\":\"record\",\"name\":\"KafkaConnection\",\"namespace\":\"com.linkedin.datastream.common\",\"fields\":[{\"name\":\"topicName\",\"type\":\"string\",\"doc\":\"Name of the kafka topic.\"},{\"name\":\"partitions\",\"type\":\"int\",\"doc\":\"Number of partitions of the kafka topic.\"},{\"name\":\"metadataBrokers\",\"type\":\"string\",\"doc\":\"Comma separated list of kafka metadata brokers.\"}]}"));
    private final static RecordDataSchema.Field FIELD_TopicName = SCHEMA.getField("topicName");
    private final static RecordDataSchema.Field FIELD_Partitions = SCHEMA.getField("partitions");
    private final static RecordDataSchema.Field FIELD_MetadataBrokers = SCHEMA.getField("metadataBrokers");

    public KafkaConnection() {
        super(new DataMap(), SCHEMA);
    }

    public KafkaConnection(DataMap data) {
        super(data, SCHEMA);
    }

    public static KafkaConnection.Fields fields() {
        return _fields;
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
    public KafkaConnection setTopicName(String value, SetMode mode) {
        putDirect(FIELD_TopicName, String.class, String.class, value, mode);
        return this;
    }

    /**
     * Setter for topicName
     * 
     * @see Fields#topicName
     */
    public KafkaConnection setTopicName(String value) {
        putDirect(FIELD_TopicName, String.class, String.class, value, SetMode.DISALLOW_NULL);
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
    public KafkaConnection setPartitions(Integer value, SetMode mode) {
        putDirect(FIELD_Partitions, Integer.class, Integer.class, value, mode);
        return this;
    }

    /**
     * Setter for partitions
     * 
     * @see Fields#partitions
     */
    public KafkaConnection setPartitions(Integer value) {
        putDirect(FIELD_Partitions, Integer.class, Integer.class, value, SetMode.DISALLOW_NULL);
        return this;
    }

    /**
     * Setter for partitions
     * 
     * @see Fields#partitions
     */
      // WARNING : DatastreamTask json serialization and deserialization fails when you enable this.
//    public KafkaConnection setPartitions(int value) {
//        putDirect(FIELD_Partitions, Integer.class, Integer.class, value, SetMode.DISALLOW_NULL);
//        return this;
//    }

    /**
     * Existence checker for metadataBrokers
     * 
     * @see Fields#metadataBrokers
     */
    public boolean hasMetadataBrokers() {
        return contains(FIELD_MetadataBrokers);
    }

    /**
     * Remover for metadataBrokers
     * 
     * @see Fields#metadataBrokers
     */
    public void removeMetadataBrokers() {
        remove(FIELD_MetadataBrokers);
    }

    /**
     * Getter for metadataBrokers
     * 
     * @see Fields#metadataBrokers
     */
    public String getMetadataBrokers(GetMode mode) {
        return obtainDirect(FIELD_MetadataBrokers, String.class, mode);
    }

    /**
     * Getter for metadataBrokers
     * 
     * @see Fields#metadataBrokers
     */
    public String getMetadataBrokers() {
        return getMetadataBrokers(GetMode.STRICT);
    }

    /**
     * Setter for metadataBrokers
     * 
     * @see Fields#metadataBrokers
     */
    public KafkaConnection setMetadataBrokers(String value, SetMode mode) {
        putDirect(FIELD_MetadataBrokers, String.class, String.class, value, mode);
        return this;
    }

    /**
     * Setter for metadataBrokers
     * 
     * @see Fields#metadataBrokers
     */
    public KafkaConnection setMetadataBrokers(String value) {
        putDirect(FIELD_MetadataBrokers, String.class, String.class, value, SetMode.DISALLOW_NULL);
        return this;
    }

    @Override
    public KafkaConnection clone()
        throws CloneNotSupportedException
    {
        return ((KafkaConnection) super.clone());
    }

    @Override
    public KafkaConnection copy()
        throws CloneNotSupportedException
    {
        return ((KafkaConnection) super.copy());
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
         * Name of the kafka topic.
         * 
         */
        public PathSpec topicName() {
            return new PathSpec(getPathComponents(), "topicName");
        }

        /**
         * Number of partitions of the kafka topic.
         * 
         */
        public PathSpec partitions() {
            return new PathSpec(getPathComponents(), "partitions");
        }

        /**
         * Comma separated list of kafka metadata brokers.
         * 
         */
        public PathSpec metadataBrokers() {
            return new PathSpec(getPathComponents(), "metadataBrokers");
        }

    }

}
