
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
@Generated(value = "com.linkedin.pegasus.generator.PegasusDataTemplateGenerator", comments = "LinkedIn Data Template. Generated from /Users/halu/work/datastream/datastream-common/src/main/pegasus/com/linkedin/datastream/common/Datastream.pdsc.", date = "Thu Aug 20 11:20:38 PDT 2015")
public class KafkaConnection
    extends RecordTemplate
{

    private final static KafkaConnection.Fields _fields = new KafkaConnection.Fields();
    private final static RecordDataSchema SCHEMA = ((RecordDataSchema) DataTemplateUtil.parseSchema("{\"type\":\"record\",\"name\":\"KafkaConnection\",\"namespace\":\"com.linkedin.datastream.common\",\"fields\":[{\"name\":\"topicName\",\"type\":\"string\",\"doc\":\"Name of the kafka topic.\"},{\"name\":\"kafkaMetadataBrokers\",\"type\":\"string\",\"doc\":\"Comma separated list of kafka metadata brokers.\"}]}"));
    private final static RecordDataSchema.Field FIELD_TopicName = SCHEMA.getField("topicName");
    private final static RecordDataSchema.Field FIELD_KafkaMetadataBrokers = SCHEMA.getField("kafkaMetadataBrokers");

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
    public KafkaConnection setKafkaMetadataBrokers(String value, SetMode mode) {
        putDirect(FIELD_KafkaMetadataBrokers, String.class, String.class, value, mode);
        return this;
    }

    /**
     * Setter for kafkaMetadataBrokers
     * 
     * @see Fields#kafkaMetadataBrokers
     */
    public KafkaConnection setKafkaMetadataBrokers(String value) {
        putDirect(FIELD_KafkaMetadataBrokers, String.class, String.class, value, SetMode.DISALLOW_NULL);
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
         * Comma separated list of kafka metadata brokers.
         * 
         */
        public PathSpec kafkaMetadataBrokers() {
            return new PathSpec(getPathComponents(), "kafkaMetadataBrokers");
        }

    }

}
