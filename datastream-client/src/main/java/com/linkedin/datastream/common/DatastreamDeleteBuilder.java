
package com.linkedin.datastream.common;

import com.linkedin.restli.client.RestliRequestOptions;
import com.linkedin.restli.client.base.DeleteRequestBuilderBase;
import com.linkedin.restli.common.ResourceSpec;

import javax.annotation.Generated;

@Generated(value = "com.linkedin.restli.tools.clientgen.RestRequestBuilderGenerator", comments = "LinkedIn Request Builder", date = "Wed Oct 21 21:53:45 PDT 2015")
public class DatastreamDeleteBuilder
    extends DeleteRequestBuilderBase<String, Datastream, DatastreamDeleteBuilder>
{


    public DatastreamDeleteBuilder(String baseUriTemplate, ResourceSpec resourceSpec, RestliRequestOptions requestOptions) {
        super(baseUriTemplate, Datastream.class, resourceSpec, requestOptions);
    }

}
