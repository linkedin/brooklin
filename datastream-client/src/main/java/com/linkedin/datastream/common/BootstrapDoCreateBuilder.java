
package com.linkedin.datastream.common;

import javax.annotation.Generated;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.restli.client.RestliRequestOptions;
import com.linkedin.restli.client.base.ActionRequestBuilderBase;
import com.linkedin.restli.common.ResourceSpec;


/**
 * Process the request of creating bootstrap datastream. The request provides the name of
 *  base datastream, and the server will create and return a corresponding bootstrap
 *  datastream.
 * 
 */
@Generated(value = "com.linkedin.restli.tools.clientgen.RestRequestBuilderGenerator", comments = "LinkedIn Request Builder", date = "Tue Nov 17 09:47:02 PST 2015")
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
