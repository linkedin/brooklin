
package com.linkedin.datastream.diagnostics;

import java.util.Collection;
import java.util.List;
import javax.annotation.Generated;
import com.linkedin.data.DataList;
import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.WrappingArrayTemplate;

@Generated(value = "com.linkedin.pegasus.generator.PegasusDataTemplateGenerator", comments = "LinkedIn Data Template. Generated from /Users/spunuru/datastream/ds/datastream-common/src/main/pegasus/com/linkedin/datastream/diagnostics/ServerHealth.pdsc.", date = "Mon Jan 11 19:13:50 PST 2016")
public class ConnectorHealthArray
    extends WrappingArrayTemplate<ConnectorHealth>
{

    private final static ArrayDataSchema SCHEMA = ((ArrayDataSchema) DataTemplateUtil.parseSchema("{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"ConnectorHealth\",\"namespace\":\"com.linkedin.datastream.diagnostics\",\"doc\":\"Datastream connector health\",\"fields\":[{\"name\":\"connectorType\",\"type\":\"string\",\"doc\":\"type of the connector.\"},{\"name\":\"strategy\",\"type\":\"string\",\"doc\":\"Strategy used by the connector.\"},{\"name\":\"tasks\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"TaskHealth\",\"doc\":\"Datastream connector health\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name of the task.\"},{\"name\":\"datastreams\",\"type\":\"string\",\"doc\":\"Name of the datastreams associated with the task.\"},{\"name\":\"partitions\",\"type\":\"string\",\"doc\":\"Partitions associated with the task.\"},{\"name\":\"source\",\"type\":\"string\",\"doc\":\"Source of the datastream.\"},{\"name\":\"destination\",\"type\":\"string\",\"doc\":\"Destination of the datastream.\"},{\"name\":\"status\",\"type\":\"string\",\"doc\":\"Status of the datastream task.\"},{\"name\":\"sourceCheckpoint\",\"type\":\"string\",\"doc\":\"Source checkpoint.\"}]}},\"doc\":\"tasks assigned to the connector Instance.\"}]}}"));

    public ConnectorHealthArray() {
        this(new DataList());
    }

    public ConnectorHealthArray(int initialCapacity) {
        this(new DataList(initialCapacity));
    }

    public ConnectorHealthArray(Collection<ConnectorHealth> c) {
        this(new DataList(c.size()));
        addAll(c);
    }

    public ConnectorHealthArray(DataList data) {
        super(data, SCHEMA, ConnectorHealth.class);
    }

    @Override
    public ConnectorHealthArray clone()
        throws CloneNotSupportedException
    {
        return ((ConnectorHealthArray) super.clone());
    }

    @Override
    public ConnectorHealthArray copy()
        throws CloneNotSupportedException
    {
        return ((ConnectorHealthArray) super.copy());
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

        public com.linkedin.datastream.diagnostics.ConnectorHealth.Fields items() {
            return new com.linkedin.datastream.diagnostics.ConnectorHealth.Fields(getPathComponents(), PathSpec.WILDCARD);
        }

    }

}
