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
    String magicValue0 = KafkaPassthroughRecordMagicConverter.convertMagicToString(RecordBatch.MAGIC_VALUE_V0);
    Assert.assertEquals(new byte[] { RecordBatch.MAGIC_VALUE_V0 },
        KafkaPassthroughRecordMagicConverter.convertMagicStringToByteArray(magicValue0));

    String magicValue1 = KafkaPassthroughRecordMagicConverter.convertMagicToString(RecordBatch.MAGIC_VALUE_V1);
    Assert.assertEquals(new byte[] { RecordBatch.MAGIC_VALUE_V1 },
        KafkaPassthroughRecordMagicConverter.convertMagicStringToByteArray(magicValue1));

    String magicValue2 = KafkaPassthroughRecordMagicConverter.convertMagicToString(RecordBatch.MAGIC_VALUE_V2);
    Assert.assertEquals(new byte[] { RecordBatch.MAGIC_VALUE_V2 },
        KafkaPassthroughRecordMagicConverter.convertMagicStringToByteArray(magicValue2));

    String magicValueCurrent =
        KafkaPassthroughRecordMagicConverter.convertMagicToString(RecordBatch.CURRENT_MAGIC_VALUE);
    Assert.assertEquals(new byte[] { RecordBatch.CURRENT_MAGIC_VALUE },
        KafkaPassthroughRecordMagicConverter.convertMagicStringToByteArray(magicValueCurrent));
  }
}
