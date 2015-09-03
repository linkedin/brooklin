
package com.linkedin.datastream.common;

import java.util.List;
import javax.annotation.Generated;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.UnionDataSchema;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.GetMode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.data.template.UnionTemplate;


/**
 * Extensible data model of Datastream
 * 
 */
@Generated(value = "com.linkedin.pegasus.generator.PegasusDataTemplateGenerator", comments = "LinkedIn Data Template. Generated from /Users/halu/work/datastream/datastream-common/src/main/pegasus/com/linkedin/datastream/common/Datastream.pdsc.", date = "Tue Sep 01 23:38:46 PDT 2015")
public class Datastream
    extends RecordTemplate
{

    private final static Fields _fields = new Fields();
    private final static RecordDataSchema SCHEMA = ((RecordDataSchema) DataTemplateUtil.parseSchema("{\"type\":\"record\",\"name\":\"Datastream\",\"namespace\":\"com.linkedin.datastream.common\",\"doc\":\"Extensible data model of Datastream\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"Name of the Datastream.\"},{\"name\":\"connectorType\",\"type\":\"string\",\"doc\":\"Type of the connector to be used for reading the change capture events from the source, e.g. Oracle-Change, Espresso-Change, Oracle-Bootstrap, Espresso-Bootstrap, Mysql-Change etc..\"},{\"name\":\"source\",\"type\":\"string\",\"doc\":\"Source string that connector can use to connect to the data store and consume the change data.\"},{\"name\":\"target\",\"type\":[{\"type\":\"record\",\"name\":\"KafkaConnection\",\"fields\":[{\"name\":\"topicName\",\"type\":\"string\",\"doc\":\"Name of the kafka topic.\"},{\"name\":\"metadataBrokers\",\"type\":\"string\",\"doc\":\"Comma separated list of kafka metadata brokers.\"}]}],\"doc\":\"Target kafka cluster and topic that the connector will use for this Datastream. If BYOT (bring your own topic) is not enabled, this will be populated by connector by default.\",\"optional\":true},{\"name\":\"metadata\",\"type\":{\"type\":\"map\",\"values\":\"string\"},\"doc\":\"Generic metadata for Datastream (e.g. owner, expiration, etc). Metadatas are stored as user defined name/value pair.\",\"optional\":true}]}"));
    private final static RecordDataSchema.Field FIELD_Name = SCHEMA.getField("name");
    private final static RecordDataSchema.Field FIELD_ConnectorType = SCHEMA.getField("connectorType");
    private final static RecordDataSchema.Field FIELD_Source = SCHEMA.getField("source");
    private final static RecordDataSchema.Field FIELD_Target = SCHEMA.getField("target");
    private final static RecordDataSchema.Field FIELD_Metadata = SCHEMA.getField("metadata");

    public Datastream() {
        super(new DataMap(), SCHEMA);
    }

    public Datastream(DataMap data) {
        super(data, SCHEMA);
    }

    public static Fields fields() {
        return _fields;
    }

    /**
     * Existence checker for name
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#name
     */
    public boolean hasName() {
        return contains(FIELD_Name);
    }

    /**
     * Remover for name
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#name
     */
    public void removeName() {
        remove(FIELD_Name);
    }

    /**
     * Getter for name
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#name
     */
    public String getName(GetMode mode) {
        return obtainDirect(FIELD_Name, String.class, mode);
    }

    /**
     * Getter for name
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#name
     */
    public String getName() {
        return getName(GetMode.STRICT);
    }

    /**
     * Setter for name
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#name
     */
    public Datastream setName(String value, SetMode mode) {
        putDirect(FIELD_Name, String.class, String.class, value, mode);
        return this;
    }

    /**
     * Setter for name
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#name
     */
    public Datastream setName(String value) {
        putDirect(FIELD_Name, String.class, String.class, value, SetMode.DISALLOW_NULL);
        return this;
    }

    /**
     * Existence checker for connectorType
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#connectorType
     */
    public boolean hasConnectorType() {
        return contains(FIELD_ConnectorType);
    }

    /**
     * Remover for connectorType
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#connectorType
     */
    public void removeConnectorType() {
        remove(FIELD_ConnectorType);
    }

    /**
     * Getter for connectorType
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#connectorType
     */
    public String getConnectorType(GetMode mode) {
        return obtainDirect(FIELD_ConnectorType, String.class, mode);
    }

    /**
     * Getter for connectorType
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#connectorType
     */
    public String getConnectorType() {
        return getConnectorType(GetMode.STRICT);
    }

    /**
     * Setter for connectorType
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#connectorType
     */
    public Datastream setConnectorType(String value, SetMode mode) {
        putDirect(FIELD_ConnectorType, String.class, String.class, value, mode);
        return this;
    }

    /**
     * Setter for connectorType
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#connectorType
     */
    public Datastream setConnectorType(String value) {
        putDirect(FIELD_ConnectorType, String.class, String.class, value, SetMode.DISALLOW_NULL);
        return this;
    }

    /**
     * Existence checker for source
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#source
     */
    public boolean hasSource() {
        return contains(FIELD_Source);
    }

    /**
     * Remover for source
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#source
     */
    public void removeSource() {
        remove(FIELD_Source);
    }

    /**
     * Getter for source
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#source
     */
    public String getSource(GetMode mode) {
        return obtainDirect(FIELD_Source, String.class, mode);
    }

    /**
     * Getter for source
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#source
     */
    public String getSource() {
        return getSource(GetMode.STRICT);
    }

    /**
     * Setter for source
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#source
     */
    public Datastream setSource(String value, SetMode mode) {
        putDirect(FIELD_Source, String.class, String.class, value, mode);
        return this;
    }

    /**
     * Setter for source
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#source
     */
    public Datastream setSource(String value) {
        putDirect(FIELD_Source, String.class, String.class, value, SetMode.DISALLOW_NULL);
        return this;
    }

    /**
     * Existence checker for target
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#target
     */
    public boolean hasTarget() {
        return contains(FIELD_Target);
    }

    /**
     * Remover for target
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#target
     */
    public void removeTarget() {
        remove(FIELD_Target);
    }

    /**
     * Getter for target
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#target
     */
    public Target getTarget(GetMode mode) {
        return obtainWrapped(FIELD_Target, Target.class, mode);
    }

    /**
     * Getter for target
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#target
     */
    public Target getTarget() {
        return getTarget(GetMode.STRICT);
    }

    /**
     * Setter for target
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#target
     */
    public Datastream setTarget(Target value, SetMode mode) {
        putWrapped(FIELD_Target, Target.class, value, mode);
        return this;
    }

    /**
     * Setter for target
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#target
     */
    public Datastream setTarget(Target value) {
        putWrapped(FIELD_Target, Target.class, value, SetMode.DISALLOW_NULL);
        return this;
    }

    /**
     * Existence checker for metadata
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#metadata
     */
    public boolean hasMetadata() {
        return contains(FIELD_Metadata);
    }

    /**
     * Remover for metadata
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#metadata
     */
    public void removeMetadata() {
        remove(FIELD_Metadata);
    }

    /**
     * Getter for metadata
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#metadata
     */
    public StringMap getMetadata(GetMode mode) {
        return obtainWrapped(FIELD_Metadata, StringMap.class, mode);
    }

    /**
     * Getter for metadata
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#metadata
     */
    public StringMap getMetadata() {
        return getMetadata(GetMode.STRICT);
    }

    /**
     * Setter for metadata
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#metadata
     */
    public Datastream setMetadata(StringMap value, SetMode mode) {
        putWrapped(FIELD_Metadata, StringMap.class, value, mode);
        return this;
    }

    /**
     * Setter for metadata
     * 
     * @see com.linkedin.datastream.common.Datastream.Fields#metadata
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
         * Source string that connector can use to connect to the data store and consume the change data.
         * 
         */
        public PathSpec source() {
            return new PathSpec(getPathComponents(), "source");
        }

        /**
         * Target kafka cluster and topic that the connector will use for this Datastream. If BYOT (bring your own topic) is not enabled, this will be populated by connector by default.
         * 
         */
        public Target.Fields target() {
            return new Target.Fields(getPathComponents(), "target");
        }

        /**
         * Generic metadata for Datastream (e.g. owner, expiration, etc). Metadatas are stored as user defined name/value pair.
         * 
         */
        public PathSpec metadata() {
            return new PathSpec(getPathComponents(), "metadata");
        }

    }

    @Generated(value = "com.linkedin.pegasus.generator.PegasusDataTemplateGenerator", comments = "LinkedIn Data Template. Generated from /Users/halu/work/datastream/datastream-common/src/main/pegasus/com/linkedin/datastream/common/Datastream.pdsc.", date = "Tue Sep 01 23:38:46 PDT 2015")
    public final static class Target
        extends UnionTemplate
    {

        private final static UnionDataSchema SCHEMA = ((UnionDataSchema) DataTemplateUtil.parseSchema("[{\"type\":\"record\",\"name\":\"KafkaConnection\",\"namespace\":\"com.linkedin.datastream.common\",\"fields\":[{\"name\":\"topicName\",\"type\":\"string\",\"doc\":\"Name of the kafka topic.\"},{\"name\":\"metadataBrokers\",\"type\":\"string\",\"doc\":\"Comma separated list of kafka metadata brokers.\"}]}]"));
        private final static DataSchema MEMBER_KafkaConnection = SCHEMA.getType("com.linkedin.datastream.common.KafkaConnection");

        public Target() {
            super(new DataMap(), SCHEMA);
        }

        public Target(Object data) {
            super(data, SCHEMA);
        }

        public static Target create(com.linkedin.datastream.common.KafkaConnection value) {
            Target newUnion = new Target();
            newUnion.setKafkaConnection(value);
            return newUnion;
        }

        public boolean isKafkaConnection() {
            return memberIs("com.linkedin.datastream.common.KafkaConnection");
        }

        public com.linkedin.datastream.common.KafkaConnection getKafkaConnection() {
            return obtainWrapped(MEMBER_KafkaConnection, com.linkedin.datastream.common.KafkaConnection.class, "com.linkedin.datastream.common.KafkaConnection");
        }

        public void setKafkaConnection(com.linkedin.datastream.common.KafkaConnection value) {
            selectWrapped(MEMBER_KafkaConnection, com.linkedin.datastream.common.KafkaConnection.class, "com.linkedin.datastream.common.KafkaConnection", value);
        }

        @Override
        public Target clone()
            throws CloneNotSupportedException
        {
            return ((Target) super.clone());
        }

        @Override
        public Target copy()
            throws CloneNotSupportedException
        {
            return ((Target) super.copy());
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

            public com.linkedin.datastream.common.KafkaConnection.Fields KafkaConnection() {
                return new com.linkedin.datastream.common.KafkaConnection.Fields(getPathComponents(), "com.linkedin.datastream.common.KafkaConnection");
            }

        }

    }

}
