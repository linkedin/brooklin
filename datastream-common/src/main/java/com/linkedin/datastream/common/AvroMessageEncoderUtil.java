package com.linkedin.datastream.common;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.lang.Validate;

public class AvroMessageEncoderUtil {
  public static final byte MAGIC_BYTE = 0x0;

  /**
   * generates the md5 hash of the schemaId and appends it to the given byte array.
   * the byte array representing the payload of a BrooklinEnvelope
   *
   * This is done so when the client decodes the payload, it will contain a schemaId which
   * can be used to retrieve the schema from the Schema Registry
   *
   * @param schemaId
   * @param value
   * @return
   * @throws IOException
   */
  public static byte[] encode(String schemaId, byte[] value) throws IOException {
    Validate.notNull(value, "cannot encode null byte array, schemaId: " + schemaId);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(MAGIC_BYTE);
    byte[] md5Bytes = hexToMd5(schemaId);
    out.write(md5Bytes);
    out.write(value);
    return out.toByteArray();
  }

  public static byte[] encode(String schemaId, IndexedRecord record) throws IOException {
    Validate.notNull(record, "cannot encode null Record, schemaId: " + schemaId);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(MAGIC_BYTE);
    byte[] md5Bytes = hexToMd5(schemaId);

    out.write(md5Bytes);
    BinaryEncoder encoder = new BinaryEncoder(out);
    DatumWriter<IndexedRecord> writer;
    if (record instanceof SpecificRecord) {
      writer = new SpecificDatumWriter<IndexedRecord>(record.getSchema());
    } else {
      writer = new GenericDatumWriter<IndexedRecord>(record.getSchema());
    }
    writer.write(record, encoder);
    encoder.flush(); //encoder may buffer

    return out.toByteArray();
  }

  private static byte[] hexToMd5(String s) {
    int len = s.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
    }
    return data;
  }
}
