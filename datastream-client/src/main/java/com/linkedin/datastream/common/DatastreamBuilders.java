
package com.linkedin.datastream.common;

import java.util.EnumSet;
import java.util.HashMap;
import javax.annotation.Generated;
import com.linkedin.data.template.DynamicRecordMetadata;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.restli.client.OptionsRequestBuilder;
import com.linkedin.restli.client.RestliRequestOptions;
import com.linkedin.restli.common.ResourceMethod;
import com.linkedin.restli.common.ResourceSpec;
import com.linkedin.restli.common.ResourceSpecImpl;


/**
 * generated from: com.linkedin.datastream.server.dms.DatastreamResources
 * 
 */
@Generated(value = "com.linkedin.restli.tools.clientgen.RestRequestBuilderGenerator", comments = "LinkedIn Request Builder. Generated from /Users/spunuru/datastream/ds/datastream-common/src/main/idl/com.linkedin.datastream.server.dms.datastream.restspec.json.", date = "Wed Dec 09 10:45:58 PST 2015")
public class DatastreamBuilders {

    private final String _baseUriTemplate;
    private RestliRequestOptions _requestOptions;
    private final static String ORIGINAL_RESOURCE_NAME = "datastream";
    private final static ResourceSpec _resourceSpec;

    static {
        HashMap<String, DynamicRecordMetadata> requestMetadataMap = new HashMap<String, DynamicRecordMetadata>();
        HashMap<String, DynamicRecordMetadata> responseMetadataMap = new HashMap<String, DynamicRecordMetadata>();
        HashMap<String, com.linkedin.restli.common.CompoundKey.TypeInfo> keyParts = new HashMap<String, com.linkedin.restli.common.CompoundKey.TypeInfo>();
        _resourceSpec = new ResourceSpecImpl(EnumSet.of(ResourceMethod.GET, ResourceMethod.CREATE, ResourceMethod.UPDATE, ResourceMethod.DELETE), requestMetadataMap, responseMetadataMap, String.class, null, null, Datastream.class, keyParts);
    }

    public DatastreamBuilders() {
        _baseUriTemplate = ORIGINAL_RESOURCE_NAME;
        _requestOptions = RestliRequestOptions.DEFAULT_OPTIONS;
    }

    public DatastreamBuilders(String primaryResourceName) {
        this(primaryResourceName, RestliRequestOptions.DEFAULT_OPTIONS);
    }

    public DatastreamBuilders(RestliRequestOptions requestOptions) {
        _baseUriTemplate = ORIGINAL_RESOURCE_NAME;
        _requestOptions = assignRequestOptions(requestOptions);
    }

    public DatastreamBuilders(String primaryResourceName, RestliRequestOptions requestOptions) {
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

    public DatastreamDeleteBuilder delete() {
        return new DatastreamDeleteBuilder(_baseUriTemplate, _resourceSpec, _requestOptions);
    }

    public DatastreamGetBuilder get() {
        return new DatastreamGetBuilder(_baseUriTemplate, _resourceSpec, _requestOptions);
    }

    public DatastreamCreateBuilder create() {
        return new DatastreamCreateBuilder(_baseUriTemplate, _resourceSpec, _requestOptions);
    }

    public DatastreamUpdateBuilder update() {
        return new DatastreamUpdateBuilder(_baseUriTemplate, _resourceSpec, _requestOptions);
    }

}
