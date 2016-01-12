
package com.linkedin.datastream.server.diagnostics;

import javax.annotation.Generated;
import com.linkedin.datastream.diagnostics.ServerHealth;
import com.linkedin.restli.client.RestliRequestOptions;
import com.linkedin.restli.client.base.GetRequestBuilderBase;
import com.linkedin.restli.common.ResourceSpec;

@Generated(value = "com.linkedin.restli.tools.clientgen.RestRequestBuilderGenerator", comments = "LinkedIn Request Builder", date = "Tue Jan 12 11:21:19 PST 2016")
public class HealthGetBuilder
    extends GetRequestBuilderBase<Void, ServerHealth, HealthGetBuilder>
{


    public HealthGetBuilder(String baseUriTemplate, ResourceSpec resourceSpec, RestliRequestOptions requestOptions) {
        super(baseUriTemplate, ServerHealth.class, resourceSpec, requestOptions);
    }

}
