package com.linkedin.datastream.common;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

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
  public void testCreateInstance()
      throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException,
             InvocationTargetException {
    TestReflectionUtils utils;
    utils = ReflectionUtils.createInstance(TestReflectionUtils.class.getCanonicalName());
    Assert.assertNotNull(utils);

    utils = ReflectionUtils.createInstance(TestReflectionUtils.class.getCanonicalName(), STR_ARG1);
    Assert.assertNotNull(utils);

    utils = ReflectionUtils.createInstance(TestReflectionUtils.class.getCanonicalName(), STR_ARG1, INT_ARG2);
    Assert.assertNotNull(utils);

    try {
      utils = ReflectionUtils.createInstance(TestReflectionUtils.class.getCanonicalName(), STR_ARG1, STR_ARG1);
      Assert.assertTrue(true);
    } catch (NoSuchMethodException e) {
    }

    try {
      utils = ReflectionUtils.createInstance("Foobar");
      Assert.assertTrue(true);
    } catch (ClassNotFoundException e) {
    }

    try {
      utils = ReflectionUtils.createInstance("Foobar", 200);
      Assert.assertTrue(true);
    } catch (ClassNotFoundException e) {
    }

    boolean exception = false;
    try {
      ReflectionUtils.createInstance(null);
    } catch (Exception e) {
      exception = true;
    }
    Assert.assertTrue(exception);
  }

  static class TestData {
    private String _privateField;
    public String _publicField;
  }

  @Test
  public void testSetField() throws Exception {
    TestData data = new TestData();
    Assert.assertEquals(ReflectionUtils.setField(data, "_privateField", "world"), "world");
    Assert.assertEquals("world", ReflectionUtils.getField(data, "_privateField"));

    Assert.assertEquals(ReflectionUtils.setField(data, "_publicField", "hello"), "hello");
    Assert.assertEquals("hello", ReflectionUtils.getField(data, "_publicField"));
  }

  private void privateVoidMethod(Integer foo, String bar) {
    System.out.println("privateVoidMethod: " + String.valueOf(foo) + " " + bar);
  }

  private int privateIntMethod(Integer foo) {
    System.out.println("privateIntMethod: " + String.valueOf(foo));
    return foo + 5;
  }

  public int publicNoArgsMethod() {
    System.out.println("publicPlainMethod");
    return 10;
  }

  @Test
  public void testCallMethod() throws Exception {
    TestReflectionUtils tester = new TestReflectionUtils();
    ReflectionUtils.callMethod(tester, "privateVoidMethod", 10, "Hello");
    int retVal = ReflectionUtils.callMethod(tester, "privateIntMethod", 10);
    Assert.assertEquals(retVal, 15);

    // private method should stay private
    Method method = getClass().getDeclaredMethod("privateIntMethod", Integer.class);
    Assert.assertEquals(method.isAccessible(), false);

    retVal = ReflectionUtils.callMethod(tester, "publicNoArgsMethod");
    Assert.assertEquals(retVal, 10);
  }
}
