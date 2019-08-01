/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link KafkaBrokerAddress}
 */
public class TestKafkaBrokerAddress {

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseNull() {
    KafkaBrokerAddress.valueOf(null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseEmpty() {
    KafkaBrokerAddress.valueOf("");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseNoHost() {
    KafkaBrokerAddress.valueOf(":666");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseNoPort() {
    KafkaBrokerAddress.valueOf("somewhere");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseBadPort() {
    KafkaBrokerAddress.valueOf("somewhere:notAPort");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseNegativePort() {
    KafkaBrokerAddress.valueOf("somewhere:-666");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseLargePort() {
    KafkaBrokerAddress.valueOf("somewhere:65536");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseInvalidHostname() {
    KafkaBrokerAddress.valueOf("abc-abc.invalid-tld:666");
  }

  @Test
  public void testParseInvalidHostnameWithoutStrictHostnameCheck() {
    Assert.assertEquals(new KafkaBrokerAddress("abc-abc.invalid-tld", 666), KafkaBrokerAddress.valueOf("abc-abc.invalid-tld:666", false));
  }

  @Test
  public void testParseMaxPort() {
    Assert.assertEquals(new KafkaBrokerAddress("linkedin.com", 65535), KafkaBrokerAddress.valueOf("linkedin.com:65535"));
  }

  @Test
  public void testParseWhitespaces() {
    Assert.assertEquals(new KafkaBrokerAddress("somewhere", 666), KafkaBrokerAddress.valueOf("\t  \r\nsomewhere  :\t666\n"));
  }

  @Test
  public void testParseIp() {
    Assert.assertEquals(new KafkaBrokerAddress("192.168.0.1", 666), KafkaBrokerAddress.valueOf("192.168.0.1:666"));
  }

  @Test
  public void testParseIpv6() {
    Assert.assertEquals(new KafkaBrokerAddress("::1", 666), KafkaBrokerAddress.valueOf(" ::1 :666"));
    Assert.assertEquals(new KafkaBrokerAddress("0:0:0:0:0:0:0:1", 666), KafkaBrokerAddress.valueOf("0:0:0:0:0:0:0:1:666"));
    Assert.assertEquals(new KafkaBrokerAddress("2001:0db8:85a3:0000:0000:8a2e:0370:7334", 666),
        KafkaBrokerAddress.valueOf("2001:0db8:85a3:0000:0000:8a2e:0370:7334:666"));
  }

  @Test
  public void testTostring() {
    Assert.assertEquals("somewhere:666", new KafkaBrokerAddress("somewhere", 666).toString());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseBadIpv6() {
    KafkaBrokerAddress.valueOf(":: 1:666");
  }
}
