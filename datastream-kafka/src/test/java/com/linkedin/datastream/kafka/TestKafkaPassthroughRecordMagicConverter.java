/**
 *  Copyright 2020 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.kafka;

import org.apache.kafka.common.record.RecordBatch;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link KafkaPassthroughRecordMagicConverter}.
 */
@Test
public class TestKafkaPassthroughRecordMagicConverter {

  @Test
  public void testMagicConversion() {
    byte[] magics = {
        RecordBatch.MAGIC_VALUE_V0,
        RecordBatch.MAGIC_VALUE_V1,
        RecordBatch.MAGIC_VALUE_V2,
        RecordBatch.CURRENT_MAGIC_VALUE
    };

    for (byte magic : magics) {
      String magicString = KafkaPassthroughRecordMagicConverter.convertMagicToString(magic);
      Assert.assertEquals(new byte[] { magic },
          KafkaPassthroughRecordMagicConverter.convertMagicStringToByteArray(magicString));
    }
  }
}
