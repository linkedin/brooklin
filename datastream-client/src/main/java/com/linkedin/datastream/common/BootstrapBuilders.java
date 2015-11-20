
package com.linkedin.datastream.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import javax.annotation.Generated;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.DynamicRecordMetadata;
import com.linkedin.data.template.FieldDef;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.restli.client.OptionsRequestBuilder;
import com.linkedin.restli.client.RestliRequestOptions;
import com.linkedin.restli.common.ResourceMethod;
import com.linkedin.restli.common.ResourceSpec;
import com.linkedin.restli.common.ResourceSpecImpl;


/**
 * BootstrapActionResources is the rest end point to process bootstrap datastream request
 * 
 * generated from: com.linkedin.datastream.server.dms.BootstrapActionResources
 * 
 */
@Generated(value = "com.linkedin.restli.tools.clientgen.RestRequestBuilderGenerator", comments = "LinkedIn Request Builder. Generated from /Users/spunuru/datastream/ds/datastream-common/src/main/idl/com.linkedin.datastream.server.dms.bootstrap.restspec.json.", date = "Thu Nov 19 16:41:26 PST 2015")
public class BootstrapBuilders {

    private final String _baseUriTemplate;
    private RestliRequestOptions _requestOptions;
    private final static String ORIGINAL_RESOURCE_NAME = "bootstrap";
    private final static ResourceSpec _resourceSpec;

    static {
        HashMap<String, DynamicRecordMetadata> requestMetadataMap = new HashMap<String, DynamicRecordMetadata>();
        ArrayList<FieldDef<?>> createParams = new ArrayList<FieldDef<?>>();
        createParams.add(new FieldDef<String>("baseDatastream", String.class, DataTemplateUtil.getSchema(String.class)));
        requestMetadataMap.put("create", new DynamicRecordMetadata("create", createParams));
        HashMap<String, DynamicRecordMetadata> responseMetadataMap = new HashMap<String, DynamicRecordMetadata>();
        responseMetadataMap.put("create", new DynamicRecordMetadata("create", Collections.singletonList(new FieldDef<Datastream>("value", Datastream.class, DataTemplateUtil.getSchema(Datastream.class)))));
        _resourceSpec = new ResourceSpecImpl(EnumSet.noneOf(ResourceMethod.class), requestMetadataMap, responseMetadataMap, Void.class, null, null, null, Collections.<String, Class<?>>emptyMap());
    }

    public BootstrapBuilders() {
        _baseUriTemplate = ORIGINAL_RESOURCE_NAME;
        _requestOptions = RestliRequestOptions.DEFAULT_OPTIONS;
    }

    public BootstrapBuilders(String primaryResourceName) {
        this(primaryResourceName, RestliRequestOptions.DEFAULT_OPTIONS);
    }

    public BootstrapBuilders(RestliRequestOptions requestOptions) {
        _baseUriTemplate = ORIGINAL_RESOURCE_NAME;
        _requestOptions = assignRequestOptions(requestOptions);
    }

    public BootstrapBuilders(String primaryResourceName, RestliRequestOptions requestOptions) {
        _baseUriTemplate = primaryResourceName;
        _requestOptions = assignRequestOptions(requestOptions);
    }

    private RestliRequestOptions assignRequestOptions(RestliRequestOptions requestOptions) {
        if (requestOptions == null) {
            return RestliRequestOptions.DEFAULT_OPTIONS;
        } else {
            return requestOptions;
        }
    }

    public static String getPrimaryResource() {
        return ORIGINAL_RESOURCE_NAME;
    }

    public OptionsRequestBuilder options() {
        return new OptionsRequestBuilder(_baseUriTemplate, _requestOptions);
    }

    /**
     * Process the request of creating bootstrap datastream. The request provides the name of
     *  base datastream, and the server will create and return a corresponding bootstrap
     *  datastream.
     * 
     * @return
     *     builder for the resource method
     */
    public BootstrapDoCreateBuilder actionCreate() {
        return new BootstrapDoCreateBuilder(_baseUriTemplate, Datastream.class, _resourceSpec, _requestOptions);
    }

}
