package com.linkedin.datastream.common;

import junit.framework.Assert;
import org.testng.annotations.Test;


public class TestReflectionUtils {
  private static final String STR_ARG1 = "hello";
  private static final Integer INT_ARG2 = 100;

  public TestReflectionUtils() {
  }

  public TestReflectionUtils(String arg1) {
    Assert.assertEquals(arg1, STR_ARG1);
  }

  public TestReflectionUtils(String arg1, Integer arg2) {
    Assert.assertEquals(arg1, STR_ARG1);
    Assert.assertEquals(arg2, INT_ARG2);
  }

  @Test
  public void testCreateInstance() {
    TestReflectionUtils utils;
    utils = ReflectionUtils.createInstance(TestReflectionUtils.class.getCanonicalName());
    Assert.assertNotNull(utils);

    utils = ReflectionUtils.createInstance(TestReflectionUtils.class.getCanonicalName(), STR_ARG1);
    Assert.assertNotNull(utils);

    utils = ReflectionUtils.createInstance(TestReflectionUtils.class.getCanonicalName(), STR_ARG1, INT_ARG2);
    Assert.assertNotNull(utils);

    utils = ReflectionUtils.createInstance(TestReflectionUtils.class.getCanonicalName(), STR_ARG1, STR_ARG1);
    Assert.assertNull(utils);

    utils = ReflectionUtils.createInstance("Foobar");
    Assert.assertNull(utils);

    utils = ReflectionUtils.createInstance("Foobar", 200);
    Assert.assertNull(utils);

    boolean exception = false;
    try {
      ReflectionUtils.createInstance(null);
    } catch (Exception e) {
      exception = true;
    }
    Assert.assertTrue(exception);
  }
}
