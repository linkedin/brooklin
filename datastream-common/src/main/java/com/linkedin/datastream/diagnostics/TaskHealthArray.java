
package com.linkedin.datastream.diagnostics;

import java.util.Collection;
import java.util.List;
import javax.annotation.Generated;
import com.linkedin.data.DataList;
import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.WrappingArrayTemplate;

@Generated(value = "com.linkedin.pegasus.generator.PegasusDataTemplateGenerator", comments = "LinkedIn Data Template. Generated from /Users/spunuru/datastream/ds/datastream-common/src/main/pegasus/com/linkedin/datastream/diagnostics/ConnectorHealth.pdsc.", date = "Mon Jan 11 19:13:50 PST 2016")
public class TaskHealthArray
    extends WrappingArrayTemplate<TaskHealth>
{

    private final static ArrayDataSchema SCHEMA = ((ArrayDataSchema) DataTemplateUtil.parseSchema("{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"TaskHealth\",\"namespace\":\"com.linkedin.datastream.diagnostics\",\"doc\":\"Datastream connector health\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name of the task.\"},{\"name\":\"datastreams\",\"type\":\"string\",\"doc\":\"Name of the datastreams associated with the task.\"},{\"name\":\"partitions\",\"type\":\"string\",\"doc\":\"Partitions associated with the task.\"},{\"name\":\"source\",\"type\":\"string\",\"doc\":\"Source of the datastream.\"},{\"name\":\"destination\",\"type\":\"string\",\"doc\":\"Destination of the datastream.\"},{\"name\":\"status\",\"type\":\"string\",\"doc\":\"Status of the datastream task.\"},{\"name\":\"sourceCheckpoint\",\"type\":\"string\",\"doc\":\"Source checkpoint.\"}]}}"));

    public TaskHealthArray() {
        this(new DataList());
    }

    public TaskHealthArray(int initialCapacity) {
        this(new DataList(initialCapacity));
    }

    public TaskHealthArray(Collection<TaskHealth> c) {
        this(new DataList(c.size()));
        addAll(c);
    }

    public TaskHealthArray(DataList data) {
        super(data, SCHEMA, TaskHealth.class);
    }

    @Override
    public TaskHealthArray clone()
        throws CloneNotSupportedException
    {
        return ((TaskHealthArray) super.clone());
    }

    @Override
    public TaskHealthArray copy()
        throws CloneNotSupportedException
    {
        return ((TaskHealthArray) super.copy());
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

        public com.linkedin.datastream.diagnostics.TaskHealth.Fields items() {
            return new com.linkedin.datastream.diagnostics.TaskHealth.Fields(getPathComponents(), PathSpec.WILDCARD);
        }

    }

}
