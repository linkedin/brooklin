/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link RestliUtils}
 */
public class TestRestliUtils {
  @Test
  public void testSanitizeUriValid() {
    String uri = "http://abc:1234/brooklin-service/";
    Assert.assertEquals(RestliUtils.sanitizeUri(uri), uri);
  }

  @Test
  public void testSanitizeUriNoScheme() {
    String uri = "abc:1234/brooklin-service/";
    Assert.assertEquals(RestliUtils.sanitizeUri(uri), "http://" + uri);
  }

  @Test
  public void testSanitizeUriSSL() {
    String uri = "https://abc:1234/brooklin-service/";
    Assert.assertEquals(RestliUtils.sanitizeUri(uri), uri);
  }

  @Test
  public void testSanitizeUriNoTrailingSlash() {
    String uri = "http://abc:1234/brooklin-service";
    Assert.assertEquals(RestliUtils.sanitizeUri(uri), uri + "/");
  }
}
