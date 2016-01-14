package com.linkedin.datastream.testutil.event.validator;

import com.linkedin.datastream.testutil.event.generator.UnknownTypeException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Event validator functionality
 */
public class GenericEventValidator {

  private static final Logger LOG = LoggerFactory.getLogger(GenericEventValidator.class.getName());

  static final String[] payloadStrs = {"payload"};
  static final String[] keyStrs = {"key"};

  public GenericEventValidator() {
  }

  private static Object getPayload(Object obj) {
    SpecificRecordBase record = (SpecificRecordBase) obj;
    Schema schema = record.getSchema();
    for (String name : payloadStrs) {
      Schema.Field payload = schema.getField(name);
      if (payload != null) {
        // found it
        return record.get(payload.pos());
      }
    }
    return obj;        // treat this as generic payload record
  }

  private static Object getKey(Object obj) {
    SpecificRecordBase record = (SpecificRecordBase) obj;
    Schema schema = record.getSchema();
    for (String name : keyStrs) {
      Object key = schema.getField(name);
      if (key != null) {
        // found it
        return key;
      }
    }
    return obj;        // treat this as generic payload record
  }

  public static boolean matchPayloads(Object event1, Object event2) {
    Object payload1 = getPayload(event1);
    Object payload2 = getPayload(event2);
    if (payload1 instanceof ByteBuffer) {
      // simple Object.equal() doesn't really work when it's ByteBuffer
      return Arrays.equals(((ByteBuffer) payload1).array(), ((ByteBuffer) payload2).array());
    }
    return payload1.equals(payload2);
  }

  public static boolean matchKeys(Object event1, Object event2) {
    Object key1 = getKey(event1);
    Object key2 = getKey(event2);
    if (key1 instanceof ByteBuffer) {
      // simple Object.equal() doesn't really work when it's ByteBuffer
      return Arrays.equals(((ByteBuffer) key1).array(), ((ByteBuffer) key2).array());
    }
    return (key1.equals(key2));
  }

  public static boolean matchMetadatas(Object event1, Object event2) {
    return true; // no validation needs to perform for generic events
  }

  public static boolean validateGenericEventList(List<Object> list1, List<Object> list2)
      throws UnknownTypeException, IOException {
    if (list1 == null && list2 == null) {
      return true;
    }
    if (list1 == null || list2 == null || list1.size() != list2.size()) {
      return false;
    }

    Iterator<Object> iter1 = list1.iterator();
    Iterator<Object> iter2 = list2.iterator();
    while (iter1.hasNext()) {
      Object obj1 = iter1.next();
      Object obj2 = iter2.next();
      if (!matchPayloads(obj1, obj2) || !matchKeys(obj1, obj2)) {
        LOG.error("Failed to match event: " + obj1 + " with: " + obj2);
        return false;
      }
    }
    return true;
  }
}
