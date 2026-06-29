/**
 *  Copyright 2026 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link JsonUtils}.
 */
public class TestJsonUtils {

  @Test
  public void testNewObjectMapperReturnsNonNull() {
    ObjectMapper mapper = JsonUtils.newObjectMapper();
    Assert.assertNotNull(mapper);
  }

  @Test
  public void testNewObjectMapperDisablesFailOnUnknownProperties() {
    ObjectMapper mapper = JsonUtils.newObjectMapper();
    Assert.assertFalse(mapper.isEnabled(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES),
        "JsonUtils.newObjectMapper() must disable FAIL_ON_UNKNOWN_PROPERTIES so consumers don't crash on "
            + "newer payloads during rolling deploys / schema evolution.");
  }

  @Test
  public void testNewObjectMapperToleratesUnknownProperties() throws Exception {
    ObjectMapper mapper = JsonUtils.newObjectMapper();

    // A producer adds a future field that this consumer's class doesn't know about; deserialization
    // must succeed rather than fail-loud.
    String json = "{\"name\":\"alpha\",\"futureField\":\"ignored\"}";
    SimpleBean bean = mapper.readValue(json, SimpleBean.class);
    Assert.assertEquals(bean.getName(), "alpha");
  }

  @Test
  public void testEveryNewObjectMapperCallReturnsAFreshInstance() {
    ObjectMapper a = JsonUtils.newObjectMapper();
    ObjectMapper b = JsonUtils.newObjectMapper();
    Assert.assertNotSame(a, b,
        "JsonUtils.newObjectMapper() is a factory; callers should be free to mutate (e.g. registerModule) "
            + "the returned mapper without affecting siblings.");
  }

  /**
   * Plain-old-Java bean used to test unknown-property tolerance. Only declares {@code name};
   * any extra field in the JSON payload should be silently ignored.
   */
  public static final class SimpleBean {
    private String _name;

    public String getName() {
      return _name;
    }

    public void setName(String name) {
      _name = name;
    }
  }
}
