package com.linkedin.datastream.testutil.event.validator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.testutil.event.generator.UnknownTypeException;


/**
 * Event validator functionality
 */
public class GenericEventValidator {

  static final String[] PAYLOAD_STRS = {"payload"};
  static final String[] KEY_STRS = {"key"};
  
  private static final Logger LOG = LoggerFactory.getLogger(GenericEventValidator.class.getName());

  public GenericEventValidator() {
  }

  private static Object getPayload(IndexedRecord obj) {
    Schema schema = obj.getSchema();
    for (String name : PAYLOAD_STRS) {
      Schema.Field payload = schema.getField(name);
      if (payload != null) {
        // found it
        return obj.get(payload.pos());
      }
    }
    return obj;        // treat this as generic payload record
  }

  private static Object getKey(IndexedRecord obj) {
    Schema schema = obj.getSchema();
    for (String name : KEY_STRS) {
      Object key = schema.getField(name);
      if (key != null) {
        // found it
        return key;
      }
    }
    return obj;        // treat this as generic payload record
  }

  /**
   * Checks if 2 events have matching payloads
   * @return true if payloads match
   */

  public static boolean matchPayloads(IndexedRecord event1, IndexedRecord event2) {
    Object payload1 = getPayload(event1);
    Object payload2 = getPayload(event2);
    if (payload1 instanceof ByteBuffer) {
      // simple Object.equal() doesn't really work when it's ByteBuffer
      return Arrays.equals(((ByteBuffer) payload1).array(), ((ByteBuffer) payload2).array());
    }
    return payload1.equals(payload2);
  }

  /**
   * Checks if 2 events have matching keys
   * @return true if keys match
   */
  public static boolean matchKeys(IndexedRecord event1, IndexedRecord event2) {
    Object key1 = getKey(event1);
    Object key2 = getKey(event2);
    if (key1 instanceof ByteBuffer) {
      // simple Object.equal() doesn't really work when it's ByteBuffer
      return Arrays.equals(((ByteBuffer) key1).array(), ((ByteBuffer) key2).array());
    }
    return (key1.equals(key2));
  }

  /**
   * Checks if 2 events have matching metadata
   * @return true if metadata matches
   */
  public static boolean matchMetadatas(Object event1, Object event2) {
    return true; // no validation needs to perform for generic events
  }

  /**
   * Checks if 2 lists have matching events
   * @return true if the 2 lists have matching events
   */
  public static boolean validateGenericEventList(List<? extends IndexedRecord> list1,
      List<? extends IndexedRecord> list2) throws UnknownTypeException, IOException {
    if (list1 == null && list2 == null) {
      return true;
    }
    if (list1 == null || list2 == null || list1.size() != list2.size()) {
      return false;
    }

    Iterator<? extends IndexedRecord> iter1 = list1.iterator();
    Iterator<? extends IndexedRecord> iter2 = list2.iterator();
    while (iter1.hasNext()) {
      IndexedRecord obj1 = iter1.next();
      IndexedRecord obj2 = iter2.next();
      if (!matchPayloads(obj1, obj2) || !matchKeys(obj1, obj2)) {
        LOG.error("Failed to match event: " + obj1 + " with: " + obj2);
        return false;
      }
    }
    return true;
  }
}
