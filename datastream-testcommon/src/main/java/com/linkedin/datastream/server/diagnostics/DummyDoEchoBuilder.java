
package com.linkedin.datastream.server.diagnostics;

import javax.annotation.Generated;
import com.linkedin.restli.client.RestliRequestOptions;
import com.linkedin.restli.client.base.ActionRequestBuilderBase;
import com.linkedin.restli.common.ResourceSpec;


/**
 * 
 * @deprecated
 *     This format of request builder is obsolete. Please use {@link com.linkedin.datastream.server.diagnostics.DummyDoEchoRequestBuilder} instead.
 */
@Generated(value = "com.linkedin.pegasus.generator.JavaCodeUtil", comments = "Rest.li Request Builder", date = "Mon Aug 01 15:54:46 PDT 2016")
@Deprecated
public class DummyDoEchoBuilder
    extends ActionRequestBuilderBase<Void, java.lang.String, DummyDoEchoBuilder>
{


    public DummyDoEchoBuilder(java.lang.String baseUriTemplate, Class<java.lang.String> returnClass, ResourceSpec resourceSpec, RestliRequestOptions requestOptions) {
        super(baseUriTemplate, returnClass, resourceSpec, requestOptions);
        super.name("echo");
    }

    public DummyDoEchoBuilder paramMessage(java.lang.String value) {
        super.setReqParam(_resourceSpec.getRequestMetadata("echo").getFieldDef("message"), value);
        return this;
    }

}
