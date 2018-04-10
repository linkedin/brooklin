package com.linkedin.datastream.common;

import org.testng.Assert;
import org.testng.annotations.Test;


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
