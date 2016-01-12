
package com.linkedin.datastream.server.diagnostics;

import java.util.EnumSet;
import java.util.HashMap;
import javax.annotation.Generated;
import com.linkedin.data.template.DynamicRecordMetadata;
import com.linkedin.datastream.diagnostics.ServerHealth;
import com.linkedin.restli.client.OptionsRequestBuilder;
import com.linkedin.restli.client.RestliRequestOptions;
import com.linkedin.restli.common.ResourceMethod;
import com.linkedin.restli.common.ResourceSpec;
import com.linkedin.restli.common.ResourceSpecImpl;


/**
 * generated from: com.linkedin.datastream.server.diagnostics.ServerHealthResources
 * 
 */
@Generated(value = "com.linkedin.restli.tools.clientgen.RestRequestBuilderGenerator", comments = "LinkedIn Request Builder. Generated from /Users/spunuru/datastream/ds/datastream-common/src/main/idl/com.linkedin.datastream.server.diagnostics.health.restspec.json.", date = "Tue Jan 12 11:21:19 PST 2016")
public class HealthBuilders {

    private final String _baseUriTemplate;
    private RestliRequestOptions _requestOptions;
    private final static String ORIGINAL_RESOURCE_NAME = "health";
    private final static ResourceSpec _resourceSpec;

    static {
        HashMap<String, DynamicRecordMetadata> requestMetadataMap = new HashMap<String, DynamicRecordMetadata>();
        HashMap<String, DynamicRecordMetadata> responseMetadataMap = new HashMap<String, DynamicRecordMetadata>();
        _resourceSpec = new ResourceSpecImpl(EnumSet.of(ResourceMethod.GET), requestMetadataMap, responseMetadataMap, ServerHealth.class);
    }

    public HealthBuilders() {
        _baseUriTemplate = ORIGINAL_RESOURCE_NAME;
        _requestOptions = RestliRequestOptions.DEFAULT_OPTIONS;
    }

    public HealthBuilders(String primaryResourceName) {
        this(primaryResourceName, RestliRequestOptions.DEFAULT_OPTIONS);
    }

    public HealthBuilders(RestliRequestOptions requestOptions) {
        _baseUriTemplate = ORIGINAL_RESOURCE_NAME;
        _requestOptions = assignRequestOptions(requestOptions);
    }

    public HealthBuilders(String primaryResourceName, RestliRequestOptions requestOptions) {
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

    public HealthGetBuilder get() {
        return new HealthGetBuilder(_baseUriTemplate, _resourceSpec, _requestOptions);
    }

}
