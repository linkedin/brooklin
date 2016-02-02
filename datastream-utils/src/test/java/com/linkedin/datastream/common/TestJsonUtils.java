package com.linkedin.datastream.common;

import org.testng.annotations.Test;

public class TestJsonUtils {
  static class TestClass {
    private int _foo;
    private String _bar;
    private Long _baz;

    public TestClass() {
    }

    public TestClass(int foo, String bar) {
      _foo = foo;
      _bar = bar;
      _baz = 0L;
    }

    public int getFoo() {
      return _foo;
    }

    public String getBar() {
      return _bar;
    }

    public Long getBaz() {
      return _baz;
    }
  }

  @Test
  public void testDeserializationNewFieldsNoErrors() {
    String json = "{\"foo\":100,\"bar\":\"hello\"}";
    JsonUtils.fromJson(json, TestClass.class);
  }
}
