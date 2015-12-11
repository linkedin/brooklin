
package com.linkedin.datastream.common;

import javax.annotation.Generated;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.restli.client.RestliRequestOptions;
import com.linkedin.restli.client.base.DeleteRequestBuilderBase;
import com.linkedin.restli.common.ResourceSpec;

@Generated(value = "com.linkedin.restli.tools.clientgen.RestRequestBuilderGenerator", comments = "LinkedIn Request Builder", date = "Wed Dec 09 10:45:58 PST 2015")
public class DatastreamDeleteBuilder
    extends DeleteRequestBuilderBase<String, Datastream, DatastreamDeleteBuilder>
{


    public DatastreamDeleteBuilder(String baseUriTemplate, ResourceSpec resourceSpec, RestliRequestOptions requestOptions) {
        super(baseUriTemplate, Datastream.class, resourceSpec, requestOptions);
    }

}
