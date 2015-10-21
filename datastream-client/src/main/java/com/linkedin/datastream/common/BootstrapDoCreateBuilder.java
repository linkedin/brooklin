
package com.linkedin.datastream.common;

import com.linkedin.restli.client.RestliRequestOptions;
import com.linkedin.restli.client.base.ActionRequestBuilderBase;
import com.linkedin.restli.common.ResourceSpec;

import javax.annotation.Generated;

@Generated(value = "com.linkedin.restli.tools.clientgen.RestRequestBuilderGenerator", comments = "LinkedIn Request Builder", date = "Wed Oct 21 21:53:45 PDT 2015")
public class BootstrapDoCreateBuilder
    extends ActionRequestBuilderBase<Void, Datastream, BootstrapDoCreateBuilder>
{


    public BootstrapDoCreateBuilder(String baseUriTemplate, Class<Datastream> returnClass, ResourceSpec resourceSpec, RestliRequestOptions requestOptions) {
        super(baseUriTemplate, returnClass, resourceSpec, requestOptions);
        super.name("create");
    }

    public BootstrapDoCreateBuilder paramBaseDatastream(String value) {
        super.setReqParam(_resourceSpec.getRequestMetadata("create").getFieldDef("baseDatastream"), value);
        return this;
    }

}
