
package com.linkedin.datastream.common;

import com.linkedin.restli.client.RestliRequestOptions;
import com.linkedin.restli.client.base.GetRequestBuilderBase;
import com.linkedin.restli.common.ResourceSpec;

import javax.annotation.Generated;

@Generated(value = "com.linkedin.restli.tools.clientgen.RestRequestBuilderGenerator", comments = "LinkedIn Request Builder", date = "Wed Oct 21 21:53:45 PDT 2015")
public class DatastreamGetBuilder
    extends GetRequestBuilderBase<String, Datastream, DatastreamGetBuilder>
{


    public DatastreamGetBuilder(String baseUriTemplate, ResourceSpec resourceSpec, RestliRequestOptions requestOptions) {
        super(baseUriTemplate, Datastream.class, resourceSpec, requestOptions);
    }

}
