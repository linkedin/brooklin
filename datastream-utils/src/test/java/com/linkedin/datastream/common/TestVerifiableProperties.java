package com.linkedin.datastream.common;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Properties;

public class TestVerifiableProperties {

  @Test
  public void testGetDomainProperties() {
    /*
     * BaseProperties:
     *  "domain1.name1.property1" -> "value1"
     *  "domain1.name1.property2" -> "value2"
     *  "domain2.name1.property1" -> "value3"
     */
    Properties baseProperties = new Properties();
    String prefix1 = "domain1.name1";
    String prefix2 = "domain2.name1";
    String key1 = prefix1 + ".property1";
    String key2 = prefix1 + ".property2";
    String key3 = prefix2 + ".property1";
    baseProperties.put(key1, "value1");
    baseProperties.put(key2, "value2");
    baseProperties.put(key3, "value3");
    VerifiableProperties verifiableProperties = new VerifiableProperties(baseProperties);

    /*
     * Preserving the full key string. Expected result:
     *  "domain1.name1.property1" -> "value1"
     *  "domain1.name1.property2" -> "value2"
     */
    Properties props1 = verifiableProperties.getDomainProperties(prefix1, true);
    Assert.assertEquals(props1.getProperty(key1), "value1");
    Assert.assertEquals(props1.getProperty(key2), "value2");
    Assert.assertEquals(2, props1.size());
    Assert.assertEquals(props1, verifiableProperties.getDomainProperties(prefix1 + ".", true));

    /*
     * Stripping the prefix. Expected result:
     *  "property1" -> "value3"
     */
    Properties props2 = verifiableProperties.getDomainProperties(prefix2, false);
    Assert.assertEquals(props2.getProperty("property1"), "value3");
    Assert.assertEquals(1, props2.size());

    /*
     * Retrieve all properties
     */
    Properties props3 = verifiableProperties.getDomainProperties("");
    Assert.assertEquals(props3.getProperty(key1), "value1");
    Assert.assertEquals(props3.getProperty(key2), "value2");
    Assert.assertEquals(props3.getProperty(key3), "value3");
  }
}
