
package com.linkedin.datastream.common;

import javax.annotation.Generated;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.restli.client.RestliRequestOptions;
import com.linkedin.restli.client.base.UpdateRequestBuilderBase;
import com.linkedin.restli.common.ResourceSpec;

@Generated(value = "com.linkedin.restli.tools.clientgen.RestRequestBuilderGenerator", comments = "LinkedIn Request Builder", date = "Thu Nov 19 16:41:26 PST 2015")
public class DatastreamUpdateBuilder
    extends UpdateRequestBuilderBase<String, Datastream, DatastreamUpdateBuilder>
{


    public DatastreamUpdateBuilder(String baseUriTemplate, ResourceSpec resourceSpec, RestliRequestOptions requestOptions) {
        super(baseUriTemplate, Datastream.class, resourceSpec, requestOptions);
    }

}
