package com.linkedin.datastream.cloud.storage.io;

import static org.junit.Assert.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.io.Resources;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.Before;
import org.junit.Test;

public class ScribeParquetAvroConverterTest {
  private Schema.Parser schemaParser;

  @Before
  public void setup() {
    schemaParser = new Schema.Parser();
  }

  @Test
  public void testLandingGenerateParquetStructuredAvroSchema() throws Exception {
    Schema landingSchema = schemaParser.parse(
        Resources.toString(Resources.getResource("avroschemas/LandingEvent.avsc"), StandardCharsets.UTF_8)
    );

    Schema actual = ScribeParquetAvroConverter.generateParquetStructuredAvroSchema(landingSchema, "LandingEvent");

    Schema expected = new Schema.Parser().parse(
        Resources.toString(Resources.getResource("parquetavroschemas/LandingParquetAvroSchema.avsc"), StandardCharsets.UTF_8)
    );
    assertEquals(expected, actual);
  }

  @Test
  public void testOrderCancellationConfirmationGenerateParquetStructuredAvroSchema() throws Exception {
    // Nested object schema
    Schema OrderCancellationConfirmationSchema = schemaParser.parse(
        Resources.toString(Resources.getResource("avroschemas/OrderCancellationConfirmationEvent.avsc"), StandardCharsets.UTF_8)
    );

    Schema actual = ScribeParquetAvroConverter.generateParquetStructuredAvroSchema(OrderCancellationConfirmationSchema, "OrderCancellationConfirmationEvent");

    Schema expected = new Schema.Parser().parse(
        Resources.toString(Resources.getResource("parquetavroschemas/OrderCancellationConfirmationParquetAvroSchema.avsc"), StandardCharsets.UTF_8)
    );
    assertEquals(expected, actual);
  }

  @Test
  public void testAtlasDecisionGenerateParquetStructuredAvroSchema() throws Exception {
    Schema AtlasDecisionSchema = schemaParser.parse(
        Resources.toString(Resources.getResource("avroschemas/AtlasDecisionEvent.avsc"), StandardCharsets.UTF_8)
    );

    Schema actual = ScribeParquetAvroConverter.generateParquetStructuredAvroSchema(AtlasDecisionSchema, "AtlasDecisionEvent");

    Schema expected = new Schema.Parser().parse(
        Resources.toString(Resources.getResource("parquetavroschemas/AtlasDecisionParquetAvroSchema.avsc"), StandardCharsets.UTF_8)
    );
    assertEquals(expected, actual);
  }

  @Test
  public void testLandingGenerateParquetStructuredAvroData() throws Exception {
    Schema avroLandingSchema = schemaParser.parse(
        Resources.toString(Resources.getResource("avroschemas/LandingEvent.avsc"), StandardCharsets.UTF_8)
    );

    GenericRecord input = new GenericData.Record(avroLandingSchema);

    input.put("trackingEventId", "7ca521ae-ae7b-33a3-b876-738fc6719fdd");
    input.put("scribeLogId", "8b1d4bac-797e-4a5f-a959-667deda80515");
    input.put("platform", "WEB");
    input.put("storeId", 49);
    input.put("eventTimestamp", 1494843212576L);
    input.put("libraGuid", "0ae406c8-5919-7c50-a927-7040475fd802");
    input.put("customerGuid", "01c3f8ef-316a-42ef-93cf-eda4296d6ee3");
    input.put("customerId", 4075458971L);
    input.put("deviceGuid", "0ae406c8-58de-49ce-9d4a-481a2a300902");
    input.put("loginStatus", "RECOGNIZED");
    input.put("customerStatus", null);
    input.put("transactionId", "CuQGyFkZf0yoRHA+R4BzAg==");
    input.put("phpSessionId", "45crk6foph74hejr8p8s1df0c5");
    input.put("visitGuid", "0ae406c8-5919-7c50-a927-7040475fd802");
    input.put("employeeCustomerId", null);
    input.put("employeeId", null);
    input.put("isBot", false);
    input.put("idfa", null);
    input.put("datacenter", "dev");
    input.put("pageRequestEventId", "e82f044e-4e98-3f9e-a1e4-9a3809d075b8");
    input.put("referringTransactionId", null);
    input.put("referralId", null);
    input.put("userAgent", "Mozilla/5.0 (Unknown; Linux x86_64) AppleWebKit/538.1 (KHTML, like Gecko) PhantomJS/2.0.1-development Safari/538.1");
    input.put("requestUrl", "https://wayfaircom.csnzoo.com/filters/Console-and-Sofa-Tables-l443-c414605-O74021~Rectangle-P86403~55~119.html?&curpage=2");
    input.put("referringUrl", null);
    input.put("clientIp", "192.168.1.1");
    input.put("viewTimestamp", 1494843212576L);
    input.put("marketingCategoryId", "6");
    input.put("visitorType", "Returning");
    input.put("campaignCode", "Test123");
    input.put("emailClickTransactionId", "CuQGyRVWf0yoRHA+R4BzAg==");
    input.put("marketingCampaignId", "12345");
    input.put("marketingStrategyId", "12346");
    input.put("marketingCreativeId", "12347");
    input.put("marketingDecoder", "12348");
    input.put("marketingSecondary", "12349");
    input.put("pageId", "oIz2esfD2eDgUpbwSw55eA==");

    Schema parquetAvroLandingSchema = new Schema.Parser().parse(
        Resources.toString(Resources.getResource("parquetavroschemas/LandingParquetAvroSchema.avsc"), StandardCharsets.UTF_8)
    );

    GenericRecord actual = ScribeParquetAvroConverter.generateParquetStructuredAvroData(parquetAvroLandingSchema, "LandingEvent", input);

    GenericRecord expected = new GenericData.Record(parquetAvroLandingSchema);
    expected.put("trackingEventId", "7ca521ae-ae7b-33a3-b876-738fc6719fdd");
    expected.put("scribeLogId", "8b1d4bac-797e-4a5f-a959-667deda80515");
    expected.put("platform", "WEB");
    expected.put("storeId", 49);
    expected.put("eventTimestamp", "2017-05-15 06:13:32.576 -0400");
    expected.put("libraGuid", "0ae406c8-5919-7c50-a927-7040475fd802");
    expected.put("customerGuid", "01c3f8ef-316a-42ef-93cf-eda4296d6ee3");
    expected.put("customerId", 4075458971L);
    expected.put("deviceGuid", "0ae406c8-58de-49ce-9d4a-481a2a300902");
    expected.put("loginStatus", "RECOGNIZED");
    expected.put("customerStatus", null);
    expected.put("transactionId", "CuQGyFkZf0yoRHA+R4BzAg==");
    expected.put("phpSessionId", "45crk6foph74hejr8p8s1df0c5");
    expected.put("visitGuid", "0ae406c8-5919-7c50-a927-7040475fd802");
    expected.put("employeeCustomerId", null);
    expected.put("employeeId", null);
    expected.put("isBot", false);
    expected.put("idfa", null);
    expected.put("datacenter", "dev");
    expected.put("pageRequestEventId", "e82f044e-4e98-3f9e-a1e4-9a3809d075b8");
    expected.put("referringTransactionId", null);
    expected.put("referralId", null);
    expected.put("userAgent", "Mozilla/5.0 (Unknown; Linux x86_64) AppleWebKit/538.1 (KHTML, like Gecko) PhantomJS/2.0.1-development Safari/538.1");
    expected.put("requestUrl", "https://wayfaircom.csnzoo.com/filters/Console-and-Sofa-Tables-l443-c414605-O74021~Rectangle-P86403~55~119.html?&curpage=2");
    expected.put("referringUrl", null);
    expected.put("clientIp", "192.168.1.1");
    expected.put("viewTimestamp", "2017-05-15 06:13:32.576 -0400");
    expected.put("marketingCategoryId", "6");
    expected.put("visitorType", "Returning");
    expected.put("campaignCode", "Test123");
    expected.put("emailClickTransactionId", "CuQGyRVWf0yoRHA+R4BzAg==");
    expected.put("marketingCampaignId", "12345");
    expected.put("marketingStrategyId", "12346");
    expected.put("marketingCreativeId", "12347");
    expected.put("marketingDecoder", "12348");
    expected.put("marketingSecondary", "12349");
    expected.put("pageId", "oIz2esfD2eDgUpbwSw55eA==");

    assertEquals(expected, actual);
  }

  @Test
  public void testAtlasDecisionGenerateParquetStructuredAvroData() throws Exception {

    Schema avroAtlasDecisionSchema = schemaParser.parse(
        Resources.toString(Resources.getResource("avroschemas/AtlasDecisionEvent.avsc"), StandardCharsets.UTF_8)
    );

    GenericRecord input = new GenericData.Record(avroAtlasDecisionSchema);

    input.put("trackingEventId", "7ca521ae-ae7b-33a3-b876-738fc6719fdd");
    input.put("scribeLogId", "8b1d4bac-797e-4a5f-a959-667deda80515");
    input.put("platform", "WEB");
    input.put("storeId", 49);
    input.put("eventTimestamp", 1494843212576L);
    input.put("libraGuid", "0ae406c8-5919-7c50-a927-7040475fd802");
    input.put("transactionId", "CuQGyFkZf0yoRHA+R4BzAg==");
    input.put("referringTransactionId", "CuQGyHkZf0yoRHA+R4BzAg==");
    input.put("datacenter", "test");
    input.put("visitGuid", "01c3f8ef-316a-42ef-93cf-eda4296d6ee3");
    input.put("deviceGuid", "0ae406c8-58de-49ce-9d4a-481a2a300902");
    input.put("customerGuid", "0ae406c8-58de-49ce-9d4a-481a2a300902");
    input.put("adid", "01c3f8ef-316a-42ef-93cf-eda4296d6ee3");
    input.put("idfa", "0ae406c8-58de-49ce-9d4a-481a2a300902");
    input.put("idfv", "0ae406c8-58de-49ce-9d4a-481a2a300902");
    input.put("atlasPlatformId", 1234567L);
    input.put("atlasChannelId", 324L);


    List<Long> listLong = new ArrayList<Long>();
    listLong.add(12345678L);
    listLong.add(87654321L);
    GenericArray<Long> placementIds = new GenericData.Array<>(Schema.createArray(Schema.create(Schema.Type.LONG)), listLong);
    input.put("placementIds",  placementIds);

    GenericArray<Long> campaignIds = new GenericData.Array<Long>(Schema.createArray(Schema.create(Schema.Type.LONG)), listLong);
    input.put("campaignIds",  campaignIds);

    GenericArray<Long> creativeIds = new GenericData.Array<Long>(Schema.createArray(Schema.create(Schema.Type.LONG)), listLong);
    input.put("creativeIds",  creativeIds);

    List<Utf8> list = new ArrayList<>();
    list.add(new Utf8("e82f044e-4e98-3f9e-a1e4-9a3809d075b8"));
    list.add(new Utf8("01c3f8ef-316a-42ef-93cf-eda4296d6ee3"));
    GenericArray<Utf8> segmentGuids = new GenericData.Array<>(Schema.createArray(Schema.create(Schema.Type.STRING)), list);
    input.put("segmentGuids",  segmentGuids);

    input.put("kairosTopFunctionalNeed", "kairos");
    input.put("kairosTopNeedId", 1234567L);

    List<Integer> listInteger = new ArrayList<Integer>();
    listInteger.add(49);
    listInteger.add(450);
    GenericArray<Integer> inMarketClassIds = new GenericData.Array<>(Schema.createArray(Schema.create(Schema.Type.INT)), listInteger);
    input.put("inMarketClassIds", inMarketClassIds);

    GenericArray<Utf8> impressionIds = new GenericData.Array<>(Schema.createArray(Schema.create(Schema.Type.STRING)), list);
    input.put("impressionIds",  impressionIds);

    GenericArray<Utf8> requestTokens = new GenericData.Array<>(Schema.createArray(Schema.create(Schema.Type.STRING)), list);
    input.put("requestTokens",  requestTokens);

    GenericArray<Utf8> arbitraryContentGuids = new GenericData.Array<>(Schema.createArray(Schema.create(Schema.Type.STRING)), list);
    input.put("arbitraryContentGuids",  arbitraryContentGuids);

    GenericArray<Utf8> externalCreativeIds = new GenericData.Array<>(Schema.createArray(Schema.create(Schema.Type.STRING)), list);
    input.put("externalCreativeIds",  externalCreativeIds);

    Schema atlasDecisionParquetAvroSchema = new Schema.Parser().parse(
        Resources.toString(Resources.getResource("parquetavroschemas/AtlasDecisionParquetAvroSchema.avsc"), StandardCharsets.UTF_8)
    );

    GenericRecord actual = ScribeParquetAvroConverter.generateParquetStructuredAvroData(atlasDecisionParquetAvroSchema, "AtlasDecisionEvent", input);

    GenericRecord expected = new GenericData.Record(atlasDecisionParquetAvroSchema);
    expected.put("trackingEventId", "7ca521ae-ae7b-33a3-b876-738fc6719fdd");
    expected.put("scribeLogId", "8b1d4bac-797e-4a5f-a959-667deda80515");
    expected.put("platform", "WEB");
    expected.put("storeId", 49);
    expected.put("eventTimestamp", "2017-05-15 06:13:32.576 -0400");
    expected.put("libraGuid", "0ae406c8-5919-7c50-a927-7040475fd802");
    expected.put("transactionId", "CuQGyFkZf0yoRHA+R4BzAg==");
    expected.put("referringTransactionId", "CuQGyHkZf0yoRHA+R4BzAg==");
    expected.put("datacenter", "test");
    expected.put("visitGuid", "01c3f8ef-316a-42ef-93cf-eda4296d6ee3");
    expected.put("deviceGuid", "0ae406c8-58de-49ce-9d4a-481a2a300902");
    expected.put("customerGuid", "0ae406c8-58de-49ce-9d4a-481a2a300902");
    expected.put("adid", "01c3f8ef-316a-42ef-93cf-eda4296d6ee3");
    expected.put("idfa", "0ae406c8-58de-49ce-9d4a-481a2a300902");
    expected.put("idfv", "0ae406c8-58de-49ce-9d4a-481a2a300902");
    expected.put("atlasPlatformId", 1234567L);
    expected.put("atlasChannelId", 324L);
    expected.put("placementIds",  Arrays.asList(12345678L, 87654321L));
    expected.put("campaignIds", Arrays.asList(12345678L, 87654321L));
    expected.put("creativeIds", Arrays.asList(12345678L, 87654321L));
    expected.put("segmentGuids", Arrays.asList("e82f044e-4e98-3f9e-a1e4-9a3809d075b8", "01c3f8ef-316a-42ef-93cf-eda4296d6ee3"));
    expected.put("kairosTopFunctionalNeed", "kairos");
    expected.put("kairosTopNeedId", 1234567L);
    expected.put("inMarketClassIds", Arrays.asList(49, 450));
    expected.put("impressionIds", Arrays.asList("e82f044e-4e98-3f9e-a1e4-9a3809d075b8", "01c3f8ef-316a-42ef-93cf-eda4296d6ee3"));
    expected.put("requestTokens", Arrays.asList("e82f044e-4e98-3f9e-a1e4-9a3809d075b8", "01c3f8ef-316a-42ef-93cf-eda4296d6ee3"));
    expected.put("arbitraryContentGuids", Arrays.asList("e82f044e-4e98-3f9e-a1e4-9a3809d075b8", "01c3f8ef-316a-42ef-93cf-eda4296d6ee3"));
    expected.put("externalCreativeIds", Arrays.asList("e82f044e-4e98-3f9e-a1e4-9a3809d075b8", "01c3f8ef-316a-42ef-93cf-eda4296d6ee3"));

    assertEquals(expected, actual);
  }
}