
package com.linkedin.datastream.server.diagnostics;

import javax.annotation.Generated;
import com.linkedin.restli.client.RestliRequestOptions;
import com.linkedin.restli.client.base.ActionRequestBuilderBase;
import com.linkedin.restli.common.ResourceSpec;

@Generated(value = "com.linkedin.pegasus.generator.JavaCodeUtil", comments = "Rest.li Request Builder", date = "Mon Aug 01 15:54:46 PDT 2016")
public class DummyDoEchoRequestBuilder
    extends ActionRequestBuilderBase<Void, java.lang.String, DummyDoEchoRequestBuilder>
{


    public DummyDoEchoRequestBuilder(java.lang.String baseUriTemplate, Class<java.lang.String> returnClass, ResourceSpec resourceSpec, RestliRequestOptions requestOptions) {
        super(baseUriTemplate, returnClass, resourceSpec, requestOptions);
        super.name("echo");
    }

    public DummyDoEchoRequestBuilder messageParam(java.lang.String value) {
        super.setReqParam(_resourceSpec.getRequestMetadata("echo").getFieldDef("message"), value);
        return this;
    }

}
