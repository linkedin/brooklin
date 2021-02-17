package com.linkedin.datastream.cloud.storage.io;

import static org.junit.Assert.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    Schema actual = ScribeParquetAvroConverter.generateParquetStructuredAvroSchema(landingSchema);

    Schema expected = new Schema.Parser().parse(
        Resources.toString(Resources.getResource("parquetavroschemas/LandingParquetAvroSchema.avsc"), StandardCharsets.UTF_8)
    );
    assertEquals(expected, actual);
  }

  @Test
  public void testScribeInternalTestParquetStructuredAvroSchema() throws Exception {
    Schema scribeInternalTestSchema = schemaParser.parse(
        Resources.toString(Resources.getResource("avroschemas/scribeInternalTest.avsc"), StandardCharsets.UTF_8)
    );

    Schema actual = ScribeParquetAvroConverter.generateParquetStructuredAvroSchema(scribeInternalTestSchema);

    Schema expected = new Schema.Parser().parse(
        Resources.toString(Resources.getResource("parquetavroschemas/scribeInternalTestParquet.avsc"), StandardCharsets.UTF_8)
    );
    assertEquals(expected, actual);
  }

  @Test
  public void testPageViewGenerateParquetStructuredAvroSchema() throws Exception {
    Schema pageViewSchema = schemaParser.parse(
        Resources.toString(Resources.getResource("avroschemas/PageViewEvent.avsc"), StandardCharsets.UTF_8)
    );

    Schema actual = ScribeParquetAvroConverter.generateParquetStructuredAvroSchema(pageViewSchema);

    Schema expected = new Schema.Parser().parse(
        Resources.toString(Resources.getResource("parquetavroschemas/PageViewParquetAvroSchema.avsc"), StandardCharsets.UTF_8)
    );
    assertEquals(expected, actual);
  }

  @Test
  public void testLandingFlattenHeaderGenerateParquetStructuredAvroSchema() throws Exception {
    Schema landingSchema = schemaParser.parse(
        Resources.toString(Resources.getResource("avroschemas/LandingEventWithHeader.avsc"), StandardCharsets.UTF_8)
    );

    Schema actual = ScribeParquetAvroConverter.generateParquetStructuredAvroSchema(landingSchema);

    Schema expected = new Schema.Parser().parse(
        Resources.toString(Resources.getResource("parquetavroschemas/LandingWithHeaderParquetAvroSchema.avsc"), StandardCharsets.UTF_8)
    );
    assertEquals(expected, actual);
  }

  @Test
  public void testSupplyChainItemsReturnsFoundGenerateParquetStructuredAvroSchema() throws Exception {
    Schema itemsReturnRoutesFoundSchema = schemaParser.parse(
        Resources.toString(Resources.getResource("avroschemas/SupplyChainItemsReturnRoutesFound.avsc"), StandardCharsets.UTF_8)
    );

    Schema actual = ScribeParquetAvroConverter.generateParquetStructuredAvroSchema(itemsReturnRoutesFoundSchema);

    Schema expected = new Schema.Parser().parse(
        Resources.toString(Resources.getResource("parquetavroschemas/SupplyChainItemsReturnRoutesFoundWithHeaderExplodedParquetAvroSchema.avsc"), StandardCharsets.UTF_8)
    );

    assertEquals(expected, actual);
  }

  @Test
  public void testOrderCancellationConfirmationGenerateParquetStructuredAvroSchema() throws Exception {
    // Nested object schema
    Schema OrderCancellationConfirmationSchema = schemaParser.parse(
        Resources.toString(Resources.getResource("avroschemas/OrderCancellationConfirmationEvent.avsc"), StandardCharsets.UTF_8)
    );

    Schema actual = ScribeParquetAvroConverter.generateParquetStructuredAvroSchema(OrderCancellationConfirmationSchema);

    Schema expected = new Schema.Parser().parse(
        Resources.toString(Resources.getResource("parquetavroschemas/OrderCancellationConfirmationParquetAvroSchema.avsc"), StandardCharsets.UTF_8)
    );
    assertEquals(expected, actual);
  }

  @Test
  public void testAdTechProductPricingGenerateParquetStructuredAvroSchema() throws Exception {
    // Nested object schema
    Schema AdTechProductPricingSchema = schemaParser.parse(
        Resources.toString(Resources.getResource("avroschemas/AdTechProductPricingEvent.avsc"), StandardCharsets.UTF_8)
    );

    Schema actual = ScribeParquetAvroConverter.generateParquetStructuredAvroSchema(AdTechProductPricingSchema);

    Schema expected = new Schema.Parser().parse(
        Resources.toString(Resources.getResource("parquetavroschemas/AdTechProductPricingParquetAvroSchema.avsc"), StandardCharsets.UTF_8)
    );
    assertEquals(expected, actual);
  }

  @Test
  public void testAtlasDecisionGenerateParquetStructuredAvroSchema() throws Exception {
    Schema AtlasDecisionSchema = schemaParser.parse(
        Resources.toString(Resources.getResource("avroschemas/AtlasDecisionEvent.avsc"), StandardCharsets.UTF_8)
    );

    Schema actual = ScribeParquetAvroConverter.generateParquetStructuredAvroSchema(AtlasDecisionSchema);

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

    GenericRecord actual = ScribeParquetAvroConverter.generateParquetStructuredAvroData(parquetAvroLandingSchema, input);

    GenericRecord expected = new GenericData.Record(parquetAvroLandingSchema);
    expected.put("trackingeventid", "7ca521ae-ae7b-33a3-b876-738fc6719fdd");
    expected.put("scribelogid", "8b1d4bac-797e-4a5f-a959-667deda80515");
    expected.put("platform", "WEB");
    expected.put("storeid", 49);
    expected.put("eventtimestamp", "2017-05-15 06:13:32.576 -0400");
    expected.put("libraguid", "0ae406c8-5919-7c50-a927-7040475fd802");
    expected.put("customerguid", "01c3f8ef-316a-42ef-93cf-eda4296d6ee3");
    expected.put("customerid", 4075458971L);
    expected.put("deviceguid", "0ae406c8-58de-49ce-9d4a-481a2a300902");
    expected.put("loginstatus", "RECOGNIZED");
    expected.put("customerstatus", null);
    expected.put("transactionid", "CuQGyFkZf0yoRHA+R4BzAg==");
    expected.put("phpsessionid", "45crk6foph74hejr8p8s1df0c5");
    expected.put("visitguid", "0ae406c8-5919-7c50-a927-7040475fd802");
    expected.put("employeecustomerid", null);
    expected.put("employeeid", null);
    expected.put("isbot", false);
    expected.put("idfa", null);
    expected.put("datacenter", "dev");
    expected.put("pagerequesteventid", "e82f044e-4e98-3f9e-a1e4-9a3809d075b8");
    expected.put("referringtransactionid", null);
    expected.put("referralid", null);
    expected.put("useragent", "Mozilla/5.0 (Unknown; Linux x86_64) AppleWebKit/538.1 (KHTML, like Gecko) PhantomJS/2.0.1-development Safari/538.1");
    expected.put("requesturl", "https://wayfaircom.csnzoo.com/filters/Console-and-Sofa-Tables-l443-c414605-O74021~Rectangle-P86403~55~119.html?&curpage=2");
    expected.put("referringurl", null);
    expected.put("clientip", "192.168.1.1");
    expected.put("viewtimestamp", "2017-05-15 06:13:32.576 -0400");
    expected.put("marketingcategoryid", "6");
    expected.put("visitortype", "Returning");
    expected.put("campaigncode", "Test123");
    expected.put("emailclicktransactionid", "CuQGyRVWf0yoRHA+R4BzAg==");
    expected.put("marketingcampaignid", "12345");
    expected.put("marketingstrategyid", "12346");
    expected.put("marketingcreativeid", "12347");
    expected.put("marketingdecoder", "12348");
    expected.put("marketingsecondary", "12349");
    expected.put("pageid", "oIz2esfD2eDgUpbwSw55eA==");

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
    input.put("placementIds", placementIds);

    GenericArray<Long> campaignIds = new GenericData.Array<Long>(Schema.createArray(Schema.create(Schema.Type.LONG)), listLong);
    input.put("campaignIds", campaignIds);

    GenericArray<Long> creativeIds = new GenericData.Array<Long>(Schema.createArray(Schema.create(Schema.Type.LONG)), listLong);
    input.put("creativeIds", creativeIds);

    List<Utf8> list = new ArrayList<>();
    list.add(new Utf8("e82f044e-4e98-3f9e-a1e4-9a3809d075b8"));
    list.add(new Utf8("01c3f8ef-316a-42ef-93cf-eda4296d6ee3"));
    GenericArray<Utf8> segmentGuids = new GenericData.Array<>(Schema.createArray(Schema.create(Schema.Type.STRING)), list);
    input.put("segmentGuids", segmentGuids);

    input.put("kairosTopFunctionalNeed", "kairos");
    input.put("kairosTopNeedId", 1234567L);

    List<Integer> listInteger = new ArrayList<Integer>();
    listInteger.add(49);
    listInteger.add(450);
    GenericArray<Integer> inMarketClassIds = new GenericData.Array<>(Schema.createArray(Schema.create(Schema.Type.INT)), listInteger);
    input.put("inMarketClassIds", inMarketClassIds);

    GenericArray<Utf8> impressionIds = new GenericData.Array<>(Schema.createArray(Schema.create(Schema.Type.STRING)), list);
    input.put("impressionIds", impressionIds);

    GenericArray<Utf8> requestTokens = new GenericData.Array<>(Schema.createArray(Schema.create(Schema.Type.STRING)), list);
    input.put("requestTokens", requestTokens);

    GenericArray<Utf8> arbitraryContentGuids = new GenericData.Array<>(Schema.createArray(Schema.create(Schema.Type.STRING)), list);
    input.put("arbitraryContentGuids", arbitraryContentGuids);

    GenericArray<Utf8> externalCreativeIds = new GenericData.Array<>(Schema.createArray(Schema.create(Schema.Type.STRING)), list);
    input.put("externalCreativeIds", externalCreativeIds);

    Schema atlasDecisionParquetAvroSchema = new Schema.Parser().parse(
        Resources.toString(Resources.getResource("parquetavroschemas/AtlasDecisionParquetAvroSchema.avsc"), StandardCharsets.UTF_8)
    );

    GenericRecord actual = ScribeParquetAvroConverter.generateParquetStructuredAvroData(atlasDecisionParquetAvroSchema, input);

    GenericRecord expected = new GenericData.Record(atlasDecisionParquetAvroSchema);
    expected.put("trackingeventid", "7ca521ae-ae7b-33a3-b876-738fc6719fdd");
    expected.put("scribelogid", "8b1d4bac-797e-4a5f-a959-667deda80515");
    expected.put("platform", "WEB");
    expected.put("storeid", 49);
    expected.put("eventtimestamp", "2017-05-15 06:13:32.576 -0400");
    expected.put("libraguid", "0ae406c8-5919-7c50-a927-7040475fd802");
    expected.put("transactionid", "CuQGyFkZf0yoRHA+R4BzAg==");
    expected.put("referringtransactionid", "CuQGyHkZf0yoRHA+R4BzAg==");
    expected.put("datacenter", "test");
    expected.put("visitguid", "01c3f8ef-316a-42ef-93cf-eda4296d6ee3");
    expected.put("deviceguid", "0ae406c8-58de-49ce-9d4a-481a2a300902");
    expected.put("customerguid", "0ae406c8-58de-49ce-9d4a-481a2a300902");
    expected.put("adid", "01c3f8ef-316a-42ef-93cf-eda4296d6ee3");
    expected.put("idfa", "0ae406c8-58de-49ce-9d4a-481a2a300902");
    expected.put("idfv", "0ae406c8-58de-49ce-9d4a-481a2a300902");
    expected.put("atlasplatformid", 1234567L);
    expected.put("atlaschannelid", 324L);
    expected.put("placementids", Arrays.asList(12345678L, 87654321L));
    expected.put("campaignids", Arrays.asList(12345678L, 87654321L));
    expected.put("creativeids", Arrays.asList(12345678L, 87654321L));
    expected.put("segmentguids", Arrays.asList("e82f044e-4e98-3f9e-a1e4-9a3809d075b8", "01c3f8ef-316a-42ef-93cf-eda4296d6ee3"));
    expected.put("kairostopfunctionalneed", "kairos");
    expected.put("kairostopneedid", 1234567L);
    expected.put("inmarketclassids", Arrays.asList(49, 450));
    expected.put("impressionids", Arrays.asList("e82f044e-4e98-3f9e-a1e4-9a3809d075b8", "01c3f8ef-316a-42ef-93cf-eda4296d6ee3"));
    expected.put("requesttokens", Arrays.asList("e82f044e-4e98-3f9e-a1e4-9a3809d075b8", "01c3f8ef-316a-42ef-93cf-eda4296d6ee3"));
    expected.put("arbitrarycontentguids", Arrays.asList("e82f044e-4e98-3f9e-a1e4-9a3809d075b8", "01c3f8ef-316a-42ef-93cf-eda4296d6ee3"));
    expected.put("externalcreativeids", Arrays.asList("e82f044e-4e98-3f9e-a1e4-9a3809d075b8", "01c3f8ef-316a-42ef-93cf-eda4296d6ee3"));

    assertEquals(expected, actual);
  }

  public Schema getSchemaForNestedObjects(Schema schema, String nestedFieldName) {
    Schema nested = null;
    for (Schema.Field field : schema.getFields()) {
      if (field.name().equalsIgnoreCase(nestedFieldName)) {
        Schema nestedSchema = field.schema().isUnion() ? field.schema().getTypes().get(1) : field.schema();
        if (nestedSchema.getType().toString().equalsIgnoreCase("array")) {
          nested = nestedSchema.getElementType();
          break;
        } else {
          nested = nestedSchema;
          break;
        }
      }
    }
    return nested;
  }

  @Test
  public void testOrderCancellationConfirmationGenerateParquetStructuredAvroData() throws Exception {
    Schema avroOrderCancellationConfirmationSchema = schemaParser.parse(
        Resources.toString(Resources.getResource("avroschemas/OrderCancellationConfirmationEvent.avsc"), StandardCharsets.UTF_8)
    );

    Schema nested = getSchemaForNestedObjects(avroOrderCancellationConfirmationSchema, "orderProducts");
    Schema nestedSplitOrderProduct = getSchemaForNestedObjects(avroOrderCancellationConfirmationSchema, "splitOrderProducts");
    Schema nestedCancelledClearanceOrderProducts = getSchemaForNestedObjects(avroOrderCancellationConfirmationSchema, "cancelledClearanceOrderProducts");

    GenericRecord orderProductInput = null;
    if (nested != null) {
      orderProductInput = new GenericData.Record(nested);
      orderProductInput.put("orderProductId", 56656L);
      orderProductInput.put("quantity", 5L);
      orderProductInput.put("discontinued", true);
      orderProductInput.put("returnGroupId", 556L);
      orderProductInput.put("deliveryStatus", "pending");
      orderProductInput.put("estimatedDeliveryDate", 1494843212576L);
    }

    GenericRecord splitOrderProductInput = null;
    if (nestedSplitOrderProduct != null) {
      splitOrderProductInput = new GenericData.Record(nestedSplitOrderProduct);
      splitOrderProductInput.put("orderProductId", 556L);
      splitOrderProductInput.put("quantity", 58L);
      splitOrderProductInput.put("discontinued", false);
      splitOrderProductInput.put("returnGroupId", 556L);
      splitOrderProductInput.put("deliveryStatus", "pending");
      splitOrderProductInput.put("estimatedDeliveryDate", 1494843212576L);
    }

    GenericRecord cancelledClearanceOrderProductInput = null;
    if (nestedCancelledClearanceOrderProducts != null) {
      cancelledClearanceOrderProductInput = new GenericData.Record(nestedCancelledClearanceOrderProducts);
      cancelledClearanceOrderProductInput.put("orderProductId", 111556L);
      cancelledClearanceOrderProductInput.put("quantity", 9L);
      cancelledClearanceOrderProductInput.put("discontinued", true);
      cancelledClearanceOrderProductInput.put("returnGroupId", 556L);
      cancelledClearanceOrderProductInput.put("deliveryStatus", "pending");
      cancelledClearanceOrderProductInput.put("estimatedDeliveryDate", 1494843212576L);
    }

    List<GenericRecord> list = new ArrayList<>();
    list.add(orderProductInput);
    GenericArray<GenericRecord> orderProductGenericArray = new GenericData.Array<>(Schema.createArray(avroOrderCancellationConfirmationSchema.getField("orderProducts").schema()), list);

    List<GenericRecord> splitOrderProductList = new ArrayList<>();
    splitOrderProductList.add(splitOrderProductInput);
    GenericArray<GenericRecord> splitOrderProductGenericArray = new GenericData.Array<>(Schema.createArray(avroOrderCancellationConfirmationSchema.getField("splitOrderProducts").schema()), splitOrderProductList);

    List<GenericRecord> cancelledClearanceOrderProductsList = new ArrayList<>();
    cancelledClearanceOrderProductsList.add(cancelledClearanceOrderProductInput);
    GenericArray<GenericRecord> cancelledClearanceOrderProductGenericArray = new GenericData.Array<>(Schema.createArray(avroOrderCancellationConfirmationSchema.getField("cancelledClearanceOrderProducts").schema()), cancelledClearanceOrderProductsList);


    GenericRecord input = new GenericData.Record(avroOrderCancellationConfirmationSchema);

    input.put("trackingEventId", "7ca521ae-ae7b-33a3-b876-738fc6719fdd");
    input.put("scribeLogId", "8b1d4bac-797e-4a5f-a959-667deda80515");
    input.put("platform", "WEB");
    input.put("storeId", 49);
    input.put("eventTimestamp", 1494843212576L);
    input.put("libraGuid", "0ae406c8-5919-7c50-a927-7040475fd802");
    input.put("orderId", 4075458971L);
    input.put("emailAddress", "test@gmail.com");
    input.put("customerId", 4075458971L);
    input.put("orderCost", 128L);
    input.put("orderProducts", orderProductGenericArray);
    input.put("splitOrderProducts", splitOrderProductGenericArray);
    input.put("cancelledClearanceOrderProducts", cancelledClearanceOrderProductGenericArray);

    input.put("webLinkCancelledOrderProducts", Arrays.asList(
        "XQT1565",
        "TOVF1019",
        "ANDV2130"
    ));
    input.put("cancelledReplacements", Arrays.asList(345, 123));
    input.put("issueCredit", false);
    input.put("originTimestamp", 1593633699000L);
    input.put("staging", true);
    input.put("notificationType", "Test123");

    Schema parquetAvroOrderCancellationConfirmationSchema = new Schema.Parser().parse(
        Resources.toString(Resources.getResource("parquetavroschemas/OrderCancellationConfirmationParquetAvroSchema.avsc"), StandardCharsets.UTF_8)
    );

    Schema parquetNested = getSchemaForNestedObjects(parquetAvroOrderCancellationConfirmationSchema, "orderProducts");
    Schema nestedParquetSplitOrderProduct = getSchemaForNestedObjects(parquetAvroOrderCancellationConfirmationSchema, "splitOrderProducts");
    Schema nestedParquetCancelledClearanceOrderProducts = getSchemaForNestedObjects(parquetAvroOrderCancellationConfirmationSchema, "cancelledClearanceOrderProducts");

    GenericRecord expectedOrderProduct = null;
    if (nested != null) {
      expectedOrderProduct = new GenericData.Record(parquetNested);
      expectedOrderProduct.put("orderproductid", 56656L);
      expectedOrderProduct.put("quantity", 5L);
      expectedOrderProduct.put("discontinued", true);
      expectedOrderProduct.put("returngroupid", 556L);
      expectedOrderProduct.put("deliverystatus", "pending");
      expectedOrderProduct.put("estimateddeliverydate", "2017-05-15 06:13:32.576 -0400");
    }

    GenericRecord expectedSplitOrderProduct = null;
    if (nestedParquetSplitOrderProduct != null) {
      expectedSplitOrderProduct = new GenericData.Record(parquetNested);
      expectedSplitOrderProduct.put("orderproductid", 556L);
      expectedSplitOrderProduct.put("quantity", 58L);
      expectedSplitOrderProduct.put("discontinued", false);
      expectedSplitOrderProduct.put("returngroupid", 556L);
      expectedSplitOrderProduct.put("deliverystatus", "pending");
      expectedSplitOrderProduct.put("estimateddeliverydate", "2017-05-15 06:13:32.576 -0400");
    }

    GenericRecord expectedCancelledClearanceOrderProduct = null;
    if (nestedParquetCancelledClearanceOrderProducts != null) {
      expectedCancelledClearanceOrderProduct = new GenericData.Record(parquetNested);
      expectedCancelledClearanceOrderProduct.put("orderproductid", 111556L);
      expectedCancelledClearanceOrderProduct.put("quantity", 9L);
      expectedCancelledClearanceOrderProduct.put("discontinued", true);
      expectedCancelledClearanceOrderProduct.put("returngroupid", 556L);
      expectedCancelledClearanceOrderProduct.put("deliverystatus", "pending");
      expectedCancelledClearanceOrderProduct.put("estimateddeliverydate", "2017-05-15 06:13:32.576 -0400");
    }

    List<GenericRecord> expectedOrderProductsList = new ArrayList<GenericRecord>();
    expectedOrderProductsList.add(expectedOrderProduct);

    List<GenericRecord> expectedSplitOrderProductList = new ArrayList<GenericRecord>();
    expectedSplitOrderProductList.add(expectedSplitOrderProduct);

    List<GenericRecord> expectedCancelledClearanceOrderProductList = new ArrayList<GenericRecord>();
    expectedCancelledClearanceOrderProductList.add(expectedCancelledClearanceOrderProduct);

    GenericRecord actual = ScribeParquetAvroConverter.generateParquetStructuredAvroData(parquetAvroOrderCancellationConfirmationSchema, input);

    GenericRecord expected = new GenericData.Record(parquetAvroOrderCancellationConfirmationSchema);
    expected.put("trackingeventid", "7ca521ae-ae7b-33a3-b876-738fc6719fdd");
    expected.put("scribelogid", "8b1d4bac-797e-4a5f-a959-667deda80515");
    expected.put("platform", "WEB");
    expected.put("storeid", 49);
    expected.put("eventtimestamp", "2017-05-15 06:13:32.576 -0400");
    expected.put("libraguid", "0ae406c8-5919-7c50-a927-7040475fd802");
    expected.put("customerid", 4075458971L);
    expected.put("orderid", 4075458971L);
    expected.put("emailaddress", "test@gmail.com");
    expected.put("ordercost", 128L);
    expected.put("weblinkcancelledorderproducts", Arrays.asList(
        "XQT1565",
        "TOVF1019",
        "ANDV2130"
    ));
    expected.put("orderproducts", expectedOrderProductsList);
    expected.put("splitorderproducts", expectedSplitOrderProductList);
    expected.put("cancelledclearanceorderproducts", expectedCancelledClearanceOrderProductList);
    expected.put("cancelledreplacements", Arrays.asList(345, 123));
    expected.put("issuecredit", false);
    expected.put("origintimestamp", "2020-07-01 16:01:39.000 -0400");
    expected.put("staging", true);
    expected.put("notificationtype", "Test123");


    assertEquals(expected, actual);
  }

  @Test
  public void testScribeInternalTestGenerateParquetStructuredAvroData() throws Exception {
    Schema avroScribeInternalTestSchema = schemaParser.parse(
        Resources.toString(Resources.getResource("avroschemas/scribeInternalTest.avsc"), StandardCharsets.UTF_8)
    );

    Schema nestedscribeHeader = getSchemaForNestedObjects(avroScribeInternalTestSchema, "scribeHeader");
    Schema testNested = getSchemaForNestedObjects(avroScribeInternalTestSchema, "requiredNested");
    Schema testDeeplyNested = getSchemaForNestedObjects(testNested, "testDeeplyNested");

    List<Utf8> list = new ArrayList<>();
    list.add(new Utf8("deepnested-1"));
    list.add(new Utf8("deepnested-2"));
    GenericArray<Utf8> listStringDeeplyNested = new GenericData.Array<>(Schema.createArray(Schema.create(Schema.Type.STRING)), list);

    Map<String, String> testStringMap = new HashMap();
    testStringMap.put("key-1", "value-1");
    testStringMap.put("key-2", "value-2");

    GenericRecord scribeHeaderInput = null;
    if (nestedscribeHeader != null) {
      scribeHeaderInput = new GenericData.Record(nestedscribeHeader);
      scribeHeaderInput.put("scribeEventId", "7ca521ae-ae7b-33a3-b876-738fc6719fdd");
      scribeHeaderInput.put("eventName", "test");
      scribeHeaderInput.put("eventTimestamp", 1494843212576L);
      scribeHeaderInput.put("receivedTimestamp", 1494843212579L);
      scribeHeaderInput.put("dataClassification", 1);
      scribeHeaderInput.put("domain", "scribe_internal");
      scribeHeaderInput.put("avroSchemaVersion", 2);
      scribeHeaderInput.put("scribeClientVersion", "trail_1");
    }

    GenericRecord testDeeplyNestedInput = null;
    if (testDeeplyNested != null) {
      testDeeplyNestedInput = new GenericData.Record(testDeeplyNested);
      testDeeplyNestedInput.put("requiredTestStringDeeplyNested", "deeplyNested");
      testDeeplyNestedInput.put("requiredTestListStringDeeplyNested", listStringDeeplyNested);
      testDeeplyNestedInput.put("testStringDeeplyNested", "non-req");
      testDeeplyNestedInput.put("testBooleanDeeplyNested", true);
      testDeeplyNestedInput.put("testIntDeeplyNested", 45);
      testDeeplyNestedInput.put("testLongDeeplyNested", 234L);

      testDeeplyNestedInput.put("testDoubleDeeplyNested", 56.65);
      testDeeplyNestedInput.put("testUuidDeeplyNested", "e82f044e-4e98-3f9e-a1e4-9a3809d075b8");
      testDeeplyNestedInput.put("testTimestampDeeplyNested", 1494843212576L);
      testDeeplyNestedInput.put("testListStringDeeplyNested", listStringDeeplyNested);
      testDeeplyNestedInput.put("testListUuidDeeplyNested", listStringDeeplyNested);
      testDeeplyNestedInput.put("testMapStringDeeplyNested", testStringMap);
      testDeeplyNestedInput.put("testMapUuidDeeplyNested", testStringMap);
    }

    List<GenericRecord> listDeeplyNested = new ArrayList<>();
    listDeeplyNested.add(testDeeplyNestedInput);
    GenericArray<GenericRecord> deeplyNestedArray = new GenericData.Array<>(Schema.createArray(avroScribeInternalTestSchema.getField("requiredNested").schema().getField("testListDeeplyNested").schema()), listDeeplyNested);


    GenericRecord requiredNestedInput = null;
    if (testNested != null) {
      requiredNestedInput = new GenericData.Record(testNested);
      requiredNestedInput.put("requiredTestStringNested", "nested");
      requiredNestedInput.put("requiredTestListStringNested", listStringDeeplyNested);
      requiredNestedInput.put("testDeeplyNested", testDeeplyNestedInput);
      requiredNestedInput.put("testListDeeplyNested", deeplyNestedArray);
      requiredNestedInput.put("testStringNested", "pending");
      requiredNestedInput.put("testBooleanNested", false);

      requiredNestedInput.put("testIntNested", 5665);
      requiredNestedInput.put("testLongNested", 5L);
      requiredNestedInput.put("testDoubleNested", 4.7);
      requiredNestedInput.put("testUuidNested", "uuid");
      requiredNestedInput.put("testTimestampNested", 1494843212579L);
      requiredNestedInput.put("testListStringNested", listStringDeeplyNested);

      requiredNestedInput.put("testListUuidNested", listStringDeeplyNested);
      requiredNestedInput.put("testMapStringNested", testStringMap);
      requiredNestedInput.put("testMapUuidNested", testStringMap);
    }


    List<GenericRecord> listTestListNested = new ArrayList<>();
    listTestListNested.add(requiredNestedInput);
    GenericArray<GenericRecord> listTestNestedGenericArray = new GenericData.Array<>(Schema.createArray(avroScribeInternalTestSchema.getField("requiredNested").schema()), listTestListNested);

    GenericRecord input = new GenericData.Record(avroScribeInternalTestSchema);

    input.put("scribeHeader", scribeHeaderInput);
    input.put("requiredTestString", "test_string");
    input.put("requiredTestListString", listStringDeeplyNested);
    input.put("requiredNested", requiredNestedInput);
    input.put("testNested", requiredNestedInput);
    input.put("testListNested", listTestNestedGenericArray);

    input.put("testString", "testString");
    input.put("testBoolean", true);
    input.put("testInt", 12);
    input.put("testLong", 34L);
    input.put("testDouble", 23.45);
    input.put("testUuid", "method");
    input.put("testTimestamp", 1494843212579L);
    input.put("testListString", listStringDeeplyNested);
    input.put("testListUuid", listStringDeeplyNested);
    input.put("testMapString", testStringMap);
    input.put("testMapUuid", testStringMap);

    Schema parquetScribeInternalTestSchema = new Schema.Parser().parse(
        Resources.toString(Resources.getResource("parquetavroschemas/scribeInternalTestParquet.avsc"), StandardCharsets.UTF_8)
    );

    Schema testNestedExpected = getSchemaForNestedObjects(parquetScribeInternalTestSchema, "requiredNested");
    Schema testDeeplyNestedExpected = getSchemaForNestedObjects(testNested, "testDeeplyNested");

    List<Utf8> listExpected = new ArrayList<>();
    listExpected.add(new Utf8("deepnested-1"));
    listExpected.add(new Utf8("deepnested-2"));
    GenericArray<Utf8> listStringDeeplyNestedExpected = new GenericData.Array<>(Schema.createArray(Schema.create(Schema.Type.STRING)), listExpected);

    Map<String, String> testStringMapExpected = new HashMap();
    testStringMapExpected.put("key-1", "value-1");
    testStringMapExpected.put("key-2", "value-2");

    GenericRecord testDeeplyNestedExp = null;
    if (testDeeplyNestedExpected != null) {
      testDeeplyNestedExp = new GenericData.Record(testDeeplyNestedExpected);
      testDeeplyNestedExp.put("requiredTestStringDeeplyNested", "deeplyNested");
      testDeeplyNestedExp.put("requiredTestListStringDeeplyNested", listStringDeeplyNestedExpected);
      testDeeplyNestedExp.put("testStringDeeplyNested", "non-req");
      testDeeplyNestedExp.put("testBooleanDeeplyNested", true);
      testDeeplyNestedExp.put("testIntDeeplyNested", 45);
      testDeeplyNestedExp.put("testLongDeeplyNested", 234L);

      testDeeplyNestedExp.put("testDoubleDeeplyNested", 56.65);
      testDeeplyNestedExp.put("testUuidDeeplyNested", "e82f044e-4e98-3f9e-a1e4-9a3809d075b8");
      testDeeplyNestedExp.put("testTimestampDeeplyNested", "2017-05-15 06:13:32.576 -0400");
      testDeeplyNestedExp.put("testListStringDeeplyNested", listStringDeeplyNestedExpected);
      testDeeplyNestedExp.put("testListUuidDeeplyNested", listStringDeeplyNestedExpected);
      testDeeplyNestedExp.put("testMapStringDeeplyNested", testStringMapExpected);
      testDeeplyNestedExp.put("testMapUuidDeeplyNested", testStringMapExpected);
    }

    List<GenericRecord> listDeeplyNestedExpected = new ArrayList<>();
    listDeeplyNestedExpected.add(testDeeplyNestedExp);
    GenericArray<GenericRecord> deeplyNestedArrayExpected = new GenericData.Array<>(Schema.createArray(avroScribeInternalTestSchema.getField("requiredNested").schema().getField("testListDeeplyNested").schema()), listDeeplyNestedExpected);


    GenericRecord requiredNestedExp = null;
    if (testNestedExpected != null) {
      requiredNestedExp = new GenericData.Record(testNestedExpected);
      requiredNestedExp.put("requiredteststringnested", "nested");
      requiredNestedExp.put("requiredtestliststringnested", listStringDeeplyNestedExpected);
      requiredNestedExp.put("testdeeplynested", testDeeplyNestedExp);
      requiredNestedExp.put("testlistdeeplynested", deeplyNestedArrayExpected);
      requiredNestedExp.put("teststringnested", "pending");
      requiredNestedExp.put("testbooleannested", false);

      requiredNestedExp.put("testintnested", 5665);
      requiredNestedExp.put("testlongnested", 5L);
      requiredNestedExp.put("testdoublenested", 4.7);
      requiredNestedExp.put("testuuidnested", "uuid");
      requiredNestedExp.put("testtimestampnested", "2017-05-15 06:13:32.579 -0400");
      requiredNestedExp.put("testliststringnested", listStringDeeplyNestedExpected);

      requiredNestedExp.put("testlistuuidnested", listStringDeeplyNestedExpected);
      requiredNestedExp.put("testmapstringnested", testStringMapExpected);
      requiredNestedExp.put("testmapuuidnested", testStringMapExpected);
    }


    List<GenericRecord> listTestNested = new ArrayList<>();
    listTestNested.add(requiredNestedExp);
    GenericArray<GenericRecord> listTestNestedGenericArrayExp = new GenericData.Array<>(Schema.createArray(avroScribeInternalTestSchema.getField("requiredNested").schema()), listTestNested);

    GenericRecord actual = ScribeParquetAvroConverter.generateParquetStructuredAvroData(parquetScribeInternalTestSchema, input);

    GenericRecord expected = new GenericData.Record(parquetScribeInternalTestSchema);
    //exploded header fields
    expected.put("scribeeventid", "7ca521ae-ae7b-33a3-b876-738fc6719fdd");
    expected.put("eventname", "test");
    expected.put("eventtimestamp", "2017-05-15 06:13:32.576 -0400");
    expected.put("receivedtimestamp", "2017-05-15 06:13:32.579 -0400");
    expected.put("dataclassification", 1);
    expected.put("domain", "scribe_internal");
    expected.put("avroschemaversion", 2);
    expected.put("scribeclientversion", "trail_1");

    expected.put("requiredteststring", "test_string");
    expected.put("requiredtestliststring", listStringDeeplyNestedExpected);
    expected.put("requirednested", requiredNestedExp);
    expected.put("testnested", requiredNestedExp);
    expected.put("testlistnested", listTestNestedGenericArrayExp);

    expected.put("teststring", "testString");
    expected.put("testboolean", true);
    expected.put("testint", 12);
    expected.put("testlong", 34L);
    expected.put("testdouble", 23.45);
    expected.put("testuuid", "method");
    expected.put("testtimestamp", "2017-05-15 06:13:32.579 -0400");
    expected.put("testliststring", listStringDeeplyNestedExpected);
    expected.put("testlistuuid", listStringDeeplyNestedExpected);
    expected.put("testmapstring", testStringMapExpected);
    expected.put("testmapuuid", testStringMapExpected);

    assertEquals(expected, actual);
  }

  @Test
  public void testAdTechProductPricingEventGenerateParquetStructuredAvroData() throws Exception {
    Schema avroAdTechProductPricingSchema = schemaParser.parse(
        Resources.toString(Resources.getResource("avroschemas/AdTechProductPricingEvent.avsc"), StandardCharsets.UTF_8)
    );

    Schema nestedItem = getSchemaForNestedObjects(avroAdTechProductPricingSchema, "item");
    Schema nestedMinimumOrderQuantity = getSchemaForNestedObjects(avroAdTechProductPricingSchema, "minimumOrderQuantity");
    Schema nestedUnit = getSchemaForNestedObjects(avroAdTechProductPricingSchema, "unit");

    GenericRecord productPriceItemInput = null;
    if (nestedItem != null) {
      productPriceItemInput = new GenericData.Record(nestedItem);
      productPriceItemInput.put("salePriceInCents", 56656L);
      productPriceItemInput.put("b2bPriceInCents", 5L);
      productPriceItemInput.put("listPriceInCents", 56656L);
      productPriceItemInput.put("clearancePriceInCents", 556L);
      productPriceItemInput.put("msrpRrpPriceInCents", 56656L);
    }

    GenericRecord productPriceMinimumOrderQuantityInput = null;
    if (nestedMinimumOrderQuantity != null) {
      productPriceMinimumOrderQuantityInput = new GenericData.Record(nestedMinimumOrderQuantity);
      productPriceMinimumOrderQuantityInput.put("salePriceInCents", 56656L);
      productPriceMinimumOrderQuantityInput.put("b2bPriceInCents", 5L);
      productPriceMinimumOrderQuantityInput.put("listPriceInCents", 56656L);
      productPriceMinimumOrderQuantityInput.put("clearancePriceInCents", 556L);
      productPriceMinimumOrderQuantityInput.put("msrpRrpPriceInCents", 56656L);
    }

    GenericRecord productPriceUnitInput = null;
    if (nestedUnit != null) {
      productPriceUnitInput = new GenericData.Record(nestedUnit);
      productPriceUnitInput.put("salePriceInCents", 56656L);
      productPriceUnitInput.put("b2bPriceInCents", 5L);
      productPriceUnitInput.put("listPriceInCents", 56656L);
      productPriceUnitInput.put("clearancePriceInCents", 556L);
      productPriceUnitInput.put("msrpRrpPriceInCents", 56656L);
    }

    GenericRecord input = new GenericData.Record(avroAdTechProductPricingSchema);

    input.put("trackingEventId", "7ca521ae-ae7b-33a3-b876-738fc6719fdd");
    input.put("scribeLogId", "8b1d4bac-797e-4a5f-a959-667deda80515");
    input.put("platform", "WEB");
    input.put("storeId", 49);
    input.put("eventTimestamp", 1494843212576L);
    input.put("libraGuid", "0ae406c8-5919-7c50-a927-7040475fd802");
    input.put("optionCombinationId", 4075458971L);
    input.put("sku", "SKU123");
    input.put("brandCatalogId", 4075458971L);
    input.put("source", "SOURCE");
    input.put("isOnCloseOutSale", true);
    input.put("isOnSale", false);
    input.put("item", productPriceItemInput);
    input.put("minimumOrderQuantity", productPriceMinimumOrderQuantityInput);
    input.put("unit", productPriceUnitInput);

    Schema parquetAvroAdTechProductPricingSchema = new Schema.Parser().parse(
        Resources.toString(Resources.getResource("parquetavroschemas/AdTechProductPricingParquetAvroSchema.avsc"), StandardCharsets.UTF_8)
    );

    Schema nestedParqueItem = getSchemaForNestedObjects(parquetAvroAdTechProductPricingSchema, "item");
    Schema nestedParquetMinimumOrderQuantity = getSchemaForNestedObjects(parquetAvroAdTechProductPricingSchema, "minimumOrderQuantity");
    Schema nestedParquetUnit = getSchemaForNestedObjects(parquetAvroAdTechProductPricingSchema, "unit");

    GenericRecord expectedItem = null;
    if (nestedParqueItem != null) {
      expectedItem = new GenericData.Record(nestedParqueItem);
      expectedItem.put("salepriceincents", 56656L);
      expectedItem.put("b2bpriceincents", 5L);
      expectedItem.put("listpriceincents", 56656L);
      expectedItem.put("clearancepriceincents", 556L);
      expectedItem.put("msrprrppriceincents", 56656L);
    }

    GenericRecord expectedMinimumOrderQuantity = null;
    if (nestedParquetMinimumOrderQuantity != null) {
      expectedMinimumOrderQuantity = new GenericData.Record(nestedParquetMinimumOrderQuantity);
      expectedMinimumOrderQuantity.put("salepriceincents", 56656L);
      expectedMinimumOrderQuantity.put("b2bpriceincents", 5L);
      expectedMinimumOrderQuantity.put("listpriceincents", 56656L);
      expectedMinimumOrderQuantity.put("clearancepriceincents", 556L);
      expectedMinimumOrderQuantity.put("msrprrppriceincents", 56656L);
    }

    GenericRecord expectedUnit = null;
    if (nestedParquetUnit != null) {
      expectedUnit = new GenericData.Record(nestedParquetUnit);
      expectedUnit.put("salepriceincents", 56656L);
      expectedUnit.put("b2bpriceincents", 5L);
      expectedUnit.put("listpriceincents", 56656L);
      expectedUnit.put("clearancepriceincents", 556L);
      expectedUnit.put("msrprrppriceincents", 56656L);
    }

    GenericRecord actual = ScribeParquetAvroConverter.generateParquetStructuredAvroData(parquetAvroAdTechProductPricingSchema, input);

    GenericRecord expected = new GenericData.Record(parquetAvroAdTechProductPricingSchema);
    expected.put("trackingeventid", "7ca521ae-ae7b-33a3-b876-738fc6719fdd");
    expected.put("scribelogid", "8b1d4bac-797e-4a5f-a959-667deda80515");
    expected.put("platform", "WEB");
    expected.put("storeid", 49);
    expected.put("eventtimestamp", "2017-05-15 06:13:32.576 -0400");
    expected.put("libraguid", "0ae406c8-5919-7c50-a927-7040475fd802");
    expected.put("optioncombinationid", 4075458971L);
    expected.put("sku", "SKU123");
    expected.put("brandcatalogid", 4075458971L);
    expected.put("source", "SOURCE");
    expected.put("isoncloseoutsale", true);
    expected.put("isonsale", false);
    expected.put("item", expectedItem);
    expected.put("minimumorderquantity", expectedMinimumOrderQuantity);
    expected.put("unit", expectedUnit);


    assertEquals(expected, actual);
  }

  @Test
  public void testItemReturnRoutesFoundGenerateParquetStructuredAvroData() throws Exception {
    Schema avroItemReturnRoutesFoundSchema = schemaParser.parse(
        Resources.toString(Resources.getResource("avroschemas/SupplyChainItemsReturnRoutesFound.avsc"), StandardCharsets.UTF_8)
    );

    Schema nestedscribeHeader = getSchemaForNestedObjects(avroItemReturnRoutesFoundSchema, "scribeHeader");
    Schema nestedUpdatedRoute = getSchemaForNestedObjects(avroItemReturnRoutesFoundSchema, "addedRoutes");

    Schema nestedRoute = getSchemaForNestedObjects(nestedUpdatedRoute, "route");
    Schema nestedFinalRoute = getSchemaForNestedObjects(avroItemReturnRoutesFoundSchema, "finalRoutes");


    GenericRecord scribeHeaderInput = null;
    if (nestedscribeHeader != null) {
      scribeHeaderInput = new GenericData.Record(nestedscribeHeader);
      scribeHeaderInput.put("scribeEventId", "7ca521ae-ae7b-33a3-b876-738fc6719fdd");
      scribeHeaderInput.put("eventName", "item_return_routes_found");
      scribeHeaderInput.put("eventTimestamp", 1494843212576L);
      scribeHeaderInput.put("receivedTimestamp", 1494843212579L);
      scribeHeaderInput.put("dataClassification", 1);
      scribeHeaderInput.put("productApplicationToken", "token");
      scribeHeaderInput.put("domain", "supplychain");
      scribeHeaderInput.put("avroSchemaVersion", 2);
      scribeHeaderInput.put("scribeClientVersion", "trail_1");
    }

    GenericRecord routesInput = null;
    if (nestedRoute != null) {
      routesInput = new GenericData.Record(nestedRoute);
      routesInput.put("destinationId", 90);
      routesInput.put("destinationTypeId", 67);
      routesInput.put("thirdPartyCarrierId", 14);
      routesInput.put("rsId", 4);
    }


    GenericRecord updatedRoutesInput = null;
    if (nestedUpdatedRoute != null) {
      updatedRoutesInput = new GenericData.Record(nestedUpdatedRoute);
      updatedRoutesInput.put("route", routesInput);
      updatedRoutesInput.put("reason", "reson");
    }


    List<GenericRecord> addedRoutesArray = new ArrayList<>();
    addedRoutesArray.add(updatedRoutesInput);
    GenericArray<GenericRecord>  addedRoutesInput = new GenericData.Array<>(Schema.createArray(avroItemReturnRoutesFoundSchema.getField("addedRoutes").schema()), addedRoutesArray);



    List<GenericRecord> filteredRoutesArray = new ArrayList<>();
    filteredRoutesArray.add(updatedRoutesInput);
    GenericArray<GenericRecord>  filteredRoutesInput = new GenericData.Array<>(Schema.createArray(avroItemReturnRoutesFoundSchema.getField("filteredRoutes").schema()), filteredRoutesArray);

    GenericRecord finalRoutesInput = null;
    if (nestedFinalRoute != null) {
      finalRoutesInput = new GenericData.Record(nestedFinalRoute);
      finalRoutesInput.put("route", routesInput);
      finalRoutesInput.put("expectedNetRecovery", 3.4f);
      finalRoutesInput.put("grossRecovery", 1.3f);
      finalRoutesInput.put("cost", 389.67f);
      finalRoutesInput.put("currency", "dollars");
      finalRoutesInput.put("carrierShipCost", 10.99f);
      finalRoutesInput.put("marketingCategoryId", 11);
      finalRoutesInput.put("openBoxDiscount", 7.60f);
      finalRoutesInput.put("liquidationTransferFee", 9f);
      finalRoutesInput.put("gradeProbabilityNew", 11f);
      finalRoutesInput.put("gradeProbabilityOpenBox", 9.67f);
      finalRoutesInput.put("gradeProbabilityLiquidation", 2.3f);
      finalRoutesInput.put("gradeProbabilityJunk", 1.99f);
      finalRoutesInput.put("junkGradeDisposalFee", 1.1f);
      finalRoutesInput.put("warehouseProcessingCost", 3.3f);
      finalRoutesInput.put("liquidationDiscountRate", 10.99f);
      finalRoutesInput.put("openBoxDiscount", 7.60f);
      finalRoutesInput.put("liquidationTransferFee", 9f);
      finalRoutesInput.put("wholesalePrice", 200f);
      finalRoutesInput.put("salePrice", 299f);
      finalRoutesInput.put("volumeSteeringConstant", 2.3f);
      finalRoutesInput.put("boxCountMultiplier", 0.99f);
      finalRoutesInput.put("productStatusMultiplier", 11f);
      finalRoutesInput.put("adjustmentMultiplier", 7.60f);
      finalRoutesInput.put("returnReasonMultiplier", 9f);
      finalRoutesInput.put("highDamageMultiplier", 200f);
      finalRoutesInput.put("repackMultiplier", 299f);
      finalRoutesInput.put("disassemblyMultiplier", 2.3f);
    }

    List<GenericRecord> finalRoutesArray = new ArrayList<>();
    finalRoutesArray.add(finalRoutesInput);
    GenericArray<GenericRecord>  finalRoutes = new GenericData.Array<>(Schema.createArray(avroItemReturnRoutesFoundSchema.getField("finalRoutes").schema()), finalRoutesArray);

    List<Integer> listOfInt = new ArrayList<Integer>();
    listOfInt.add(1234567);
    listOfInt.add(87654);
    GenericArray<Integer> supplierAllowanceType = new GenericData.Array<>(Schema.createArray(Schema.create(Schema.Type.INT)), listOfInt);

    GenericRecord input = new GenericData.Record(avroItemReturnRoutesFoundSchema);

    input.put("scribeHeader", scribeHeaderInput);
    input.put("requestId", "test_id");
    input.put("orderProductId", 1234L);
    input.put("isIncident", false);
    input.put("canItemBeReturnedToSupplier", true);
    input.put("isFedexPickupAvailable", false);
    input.put("isFedexLocationAvailable", true);
    input.put("supplierTakeReturns", true);
    input.put("supplierCountryId", 12);
    input.put("supplierReturnCountryId", 34);
    input.put("supplierAllowanceType", supplierAllowanceType);
    input.put("supplierReturnAuthorizationMethod", "method");
    input.put("supplierCastleGateWarehouseId", 123);
    input.put("isWayfairCarrier", true);
    input.put("addedRoutes", addedRoutesInput);
    input.put("filteredRoutes", filteredRoutesInput);
    input.put("finalRoutes", finalRoutes);

    Schema parquetAvroItemsReturnRequestRouteSchema = new Schema.Parser().parse(
            Resources.toString(Resources.getResource("parquetavroschemas/SupplyChainItemsReturnRoutesFoundWithHeaderExplodedParquetAvroSchema.avsc"), StandardCharsets.UTF_8)
    );
    Schema nestedUpdatedRouteExpected = getSchemaForNestedObjects(parquetAvroItemsReturnRequestRouteSchema, "addedRoutes");

    Schema nestedRouteExpected = getSchemaForNestedObjects(nestedUpdatedRouteExpected, "route");
    Schema nestedFinalRouteExpected = getSchemaForNestedObjects(parquetAvroItemsReturnRequestRouteSchema, "finalRoutes");
    GenericRecord routesExpected = null;
    if (nestedRouteExpected != null) {
      routesExpected = new GenericData.Record(nestedRouteExpected);
      routesExpected.put("destinationid", 90);
      routesExpected.put("destinationtypeid", 67);
      routesExpected.put("thirdpartycarrierid", 14);
      routesExpected.put("rsid", 4);
    }


    GenericRecord updatedRoutesExpected = null;
    if (nestedUpdatedRouteExpected != null) {
      updatedRoutesExpected = new GenericData.Record(nestedUpdatedRouteExpected);
      updatedRoutesExpected.put("route", routesExpected);
      updatedRoutesExpected.put("reason", "reson");
    }


    List<GenericRecord> addedRoutesArrayExpected = new ArrayList<>();
    addedRoutesArrayExpected.add(updatedRoutesExpected);
    GenericArray<GenericRecord>  addedRoutesExpected = new GenericData.Array<>(Schema.createArray(parquetAvroItemsReturnRequestRouteSchema.getField("addedroutes").schema()), addedRoutesArrayExpected);



    List<GenericRecord> filteredRoutesArrayExpected = new ArrayList<>();
    filteredRoutesArrayExpected.add(updatedRoutesExpected);
    GenericArray<GenericRecord>  filteredRoutesExpected = new GenericData.Array<>(Schema.createArray(parquetAvroItemsReturnRequestRouteSchema.getField("filteredroutes").schema()), filteredRoutesArrayExpected);

    GenericRecord finalRoutesExpected = null;
    if (nestedFinalRouteExpected != null) {
      finalRoutesExpected = new GenericData.Record(nestedFinalRouteExpected);
      finalRoutesExpected.put("route", routesInput);
      finalRoutesExpected.put("expectednetrecovery", 3.4f);
      finalRoutesExpected.put("grossrecovery", 1.3f);
      finalRoutesExpected.put("cost", 389.67f);
      finalRoutesExpected.put("currency", "dollars");
      finalRoutesExpected.put("carriershipcost", 10.99f);
      finalRoutesExpected.put("marketingcategoryid", 11);

      finalRoutesExpected.put("openboxdiscount", 7.60f);
      finalRoutesExpected.put("liquidationtransferfee", 9f);
      finalRoutesExpected.put("gradeprobabilitynew", 11f);
      finalRoutesExpected.put("gradeprobabilityopenbox", 9.67f);
      finalRoutesExpected.put("gradeprobabilityliquidation", 2.3f);
      finalRoutesExpected.put("gradeprobabilityjunk", 1.99f);
      finalRoutesExpected.put("junkgradedisposalfee", 1.1f);

      finalRoutesExpected.put("warehouseprocessingcost", 3.3f);
      finalRoutesExpected.put("liquidationdiscountrate", 10.99f);
      finalRoutesExpected.put("openboxdiscount", 7.60f);
      finalRoutesExpected.put("liquidationtransferfee", 9f);
      finalRoutesExpected.put("wholesaleprice", 200f);
      finalRoutesExpected.put("saleprice", 299f);
      finalRoutesExpected.put("volumesteeringconstant", 2.3f);
      finalRoutesExpected.put("boxcountmultiplier", 0.99f);
      finalRoutesExpected.put("productstatusmultiplier", 11f);


      finalRoutesExpected.put("adjustmentmultiplier", 7.60f);
      finalRoutesExpected.put("returnreasonmultiplier", 9f);
      finalRoutesExpected.put("highdamagemultiplier", 200f);
      finalRoutesExpected.put("repackmultiplier", 299f);
      finalRoutesExpected.put("disassemblymultiplier", 2.3f);

    }

    List<GenericRecord> finalRoutesListExpected = new ArrayList<>();
    finalRoutesListExpected.add(finalRoutesExpected);
    GenericArray<GenericRecord>  finalRoutesArrayExpected = new GenericData.Array<>(Schema.createArray(avroItemReturnRoutesFoundSchema.getField("finalRoutes").schema()), finalRoutesListExpected);

    List<Integer> listOfIntExpected = new ArrayList<Integer>();
    listOfIntExpected.add(1234567);
    listOfIntExpected.add(87654);
    GenericArray<Integer> supplierAllowanceTypeExpected = new GenericData.Array<>(Schema.createArray(Schema.create(Schema.Type.INT)), listOfInt);

    GenericRecord actual = ScribeParquetAvroConverter.generateParquetStructuredAvroData(parquetAvroItemsReturnRequestRouteSchema, input);


    GenericRecord expected = new GenericData.Record(parquetAvroItemsReturnRequestRouteSchema);
    //exploded header fields
    expected.put("scribeeventid", "7ca521ae-ae7b-33a3-b876-738fc6719fdd");
    expected.put("eventname", "item_return_routes_found");
    expected.put("eventtimestamp", "2017-05-15 06:13:32.576 -0400");
    expected.put("receivedtimestamp", "2017-05-15 06:13:32.579 -0400");
    expected.put("dataclassification", 1);
    expected.put("productapplicationtoken", "token");
    expected.put("domain", "supplychain");
    expected.put("avroschemaversion", 2);
    expected.put("scribeclientversion", "trail_1");

    expected.put("requestid", "test_id");
    expected.put("orderproductid", 1234L);
    expected.put("isincident", false);
    expected.put("canitembereturnedtosupplier", true);
    expected.put("isfedexpickupavailable", false);
    expected.put("isfedexlocationavailable", true);
    expected.put("suppliertakereturns", true);
    expected.put("suppliercountryid", 12);
    expected.put("supplierreturncountryid", 34);
    expected.put("supplierallowancetype", supplierAllowanceTypeExpected);
    expected.put("supplierreturnauthorizationmethod", "method");
    expected.put("suppliercastlegatewarehouseid", 123);
    expected.put("iswayfaircarrier", true);
    expected.put("addedroutes", addedRoutesExpected);
    expected.put("filteredroutes", filteredRoutesExpected);
    expected.put("finalroutes", finalRoutesArrayExpected);

    assertEquals(expected, actual);

  }

  @Test
  public void testPageViewEventGenerateParquetStructuredAvroData() throws Exception {
    Schema avroPageViewSchema = schemaParser.parse(
        Resources.toString(Resources.getResource("avroschemas/PageViewEvent.avsc"), StandardCharsets.UTF_8)
    );

    Map<String, String> mapOfStrings = new HashMap<>();
    mapOfStrings.put("test", "test-val");
    mapOfStrings.put("test-1", "test-val-1");

    GenericRecord input = new GenericData.Record(avroPageViewSchema);
    input.put("trackingEventId", "7ca521ae-ae7b-33a3-b876-738fc6719fdd");
    input.put("scribeLogId", "8b1d4bac-797e-4a5f-a959-667deda80515");
    input.put("platform", "WEB");
    input.put("storeId", 49);
    input.put("eventTimestamp", 1494843212576L);
    input.put("libraGuid", "0ae406c8-5919-7c50-a927-7040475fd802");
    input.put("customerGuid", "6956ee16-52a7-bbca-66a2-ee74a8f2b307");
    input.put("deviceGuid", "1956ee1a-52a7-bbca-66a2-ee74a8f2b307");
    input.put("transactionId", "ChgHDVWwAypCOxjlBClpAg==");
    input.put("externalDeviceGuid", "1956ee1a-52a7-bbca-66a2-ee74a8f2b307");
    input.put("canvasHashes", mapOfStrings);


    Schema parquetAvroPageViewSchema = new Schema.Parser().parse(
        Resources.toString(Resources.getResource("parquetavroschemas/PageViewParquetAvroSchema.avsc"), StandardCharsets.UTF_8)
    );

    GenericRecord actual = ScribeParquetAvroConverter.generateParquetStructuredAvroData(parquetAvroPageViewSchema, input);

    Map<String, String> mapOfStringsExpected = new HashMap<>();
    mapOfStringsExpected.put("test", "test-val");
    mapOfStringsExpected.put("test-1", "test-val-1");

    GenericRecord expected = new GenericData.Record(parquetAvroPageViewSchema);
    expected.put("trackingeventid", "7ca521ae-ae7b-33a3-b876-738fc6719fdd");
    expected.put("scribelogid", "8b1d4bac-797e-4a5f-a959-667deda80515");
    expected.put("platform", "WEB");
    expected.put("storeid", 49);
    expected.put("eventtimestamp", "2017-05-15 06:13:32.576 -0400");
    expected.put("libraguid", "0ae406c8-5919-7c50-a927-7040475fd802");
    expected.put("customerguid", "6956ee16-52a7-bbca-66a2-ee74a8f2b307");
    expected.put("deviceguid", "1956ee1a-52a7-bbca-66a2-ee74a8f2b307");
    expected.put("transactionid", "ChgHDVWwAypCOxjlBClpAg==");
    expected.put("externaldeviceguid", "1956ee1a-52a7-bbca-66a2-ee74a8f2b307");
    expected.put("canvashashes", mapOfStringsExpected);

    assertEquals(expected, actual);
  }

  @Test
  public void testAdTechProductCatalogGenerateParquetStructuredAvroSchema() throws Exception {
    Schema landingSchema = schemaParser.parse(
        Resources.toString(Resources.getResource("avroschemas/AdTechProductCatalogEvent.avsc"), StandardCharsets.UTF_8)
    );

    Schema actual = ScribeParquetAvroConverter.generateParquetStructuredAvroSchema(landingSchema);

    Schema expected = new Schema.Parser().parse(
        Resources.toString(Resources.getResource("parquetavroschemas/AdTechProductCatalogParquetAvroSchema.avsc"), StandardCharsets.UTF_8)
    );
    assertEquals(expected, actual);
  }

  @Test
  public void testAdTechProductCatalogEventGenerateFlattenedParquetStructuredAvroData() throws Exception {
    Schema avroAdTechProductCatalogSchema = schemaParser.parse(
        Resources.toString(Resources.getResource("avroschemas/AdTechProductCatalogEvent.avsc"), StandardCharsets.UTF_8)
    );

    Schema nestedManufacturerPart = getSchemaForNestedObjects(avroAdTechProductCatalogSchema, "manufacturerPart");
    Schema nestedManufacturer = getSchemaForNestedObjects(avroAdTechProductCatalogSchema, "manufacturer");


    GenericRecord productCatalogManufacturerPartInput = null;
    if (nestedManufacturerPart != null) {
      productCatalogManufacturerPartInput = new GenericData.Record(nestedManufacturerPart);
      productCatalogManufacturerPartInput.put("id", 56656L);
      productCatalogManufacturerPartInput.put("partNumber", "test123");
      productCatalogManufacturerPartInput.put("customerFacingUpc", "yes");
    }

    GenericRecord productCatalogManufacturerInput = null;
    if (nestedManufacturer != null) {
      productCatalogManufacturerInput = new GenericData.Record(nestedManufacturer);
      productCatalogManufacturerInput.put("id", 88890L);
      productCatalogManufacturerInput.put("name", "manufacturerName");
      productCatalogManufacturerInput.put("viewTimestamp", 1494843212576L);
    }

    GenericRecord input = new GenericData.Record(avroAdTechProductCatalogSchema);

    input.put("trackingEventId", "7ca521ae-ae7b-33a3-b876-738fc6719fdd");
    input.put("scribeLogId", "8b1d4bac-797e-4a5f-a959-667deda80515");
    input.put("platform", "WEB");
    input.put("storeId", 49);
    input.put("eventTimestamp", 1494843212576L);
    input.put("libraGuid", "0ae406c8-5919-7c50-a927-7040475fd802");
    input.put("optionCombinationId", 4075458971L);
    input.put("sku", "SKU123");
    input.put("brandCatalogId", 4075458971L);
    input.put("source", "SOURCE");
    input.put("displaySku", "sku876");
    input.put("name", "nametest");
    input.put("marketingCategory", "testCategory");
    input.put("status", 4L);
    input.put("statusFlag", "statusTemp");
    input.put("webDescription", "describe lklmk");
    input.put("romanceCopy", "romanceCopy");
    input.put("reviewRating", 4.4);
    input.put("reviewRating2", 3.9);
    input.put("numberOfRatings", 100L);
    input.put("salesCount", 9988L);
    input.put("currency", "dollars");
    input.put("skuManufacturerPartNumber", "statusTemp");
    input.put("whiteLabelPartNumber", "describe lklmk");
    input.put("isNew", true);
    input.put("isKit", true);
    input.put("isB2BOnly", false);
    input.put("isFindable", true);
    input.put("isAutoCreate", false);
    input.put("hasFreeGroundShipping", false);
    // nested objects
    input.put("masterClass", null);
    input.put("productClasses", null);
    input.put("options",  null);
    input.put("productDimensions",  null);
    input.put("manufacturer", productCatalogManufacturerInput);
    input.put("manufacturerPart", productCatalogManufacturerPartInput);
    input.put("supplierParts",  null);
    input.put("promotion",  null);
    input.put("isAdminOnly", true);
    input.put("prRestrictionsBitmap", null);
    input.put("pwrBayesianRating", 4.3);
    input.put("isSwatchOrderable", false);
    input.put("dimensionalInfo", null);
    input.put("proProduct", null);
    input.put("exposureWarning", null);

    Schema parquetAvroAdTechProductCatalogSchema = new Schema.Parser().parse(
        Resources.toString(Resources.getResource("parquetavroschemas/AdTechProductCatalogParquetAvroSchema.avsc"), StandardCharsets.UTF_8)
    );

    Schema nestedManufacturerExpected = getSchemaForNestedObjects(parquetAvroAdTechProductCatalogSchema, "manufacturer");
    Schema nestedManufacturerPartExpected = getSchemaForNestedObjects(parquetAvroAdTechProductCatalogSchema, "manufacturerpart");

    GenericRecord expectedManufacturer = null;
    if (nestedManufacturerExpected != null) {
      expectedManufacturer = new GenericData.Record(nestedManufacturerExpected);
      expectedManufacturer.put("id", 88890L);
      expectedManufacturer.put("name", "manufacturerName");
      expectedManufacturer.put("viewtimestamp", "2017-05-15 06:13:32.576 -0400");
    }

    GenericRecord expectedproductCatalogManufacturerPart = null;
    if (nestedManufacturerPartExpected != null) {
      expectedproductCatalogManufacturerPart = new GenericData.Record(nestedManufacturerPartExpected);
      expectedproductCatalogManufacturerPart.put("id", 56656L);
      expectedproductCatalogManufacturerPart.put("partnumber", "test123");
      expectedproductCatalogManufacturerPart.put("customerfacingupc", "yes");
    }

    GenericRecord actual = ScribeParquetAvroConverter.generateParquetStructuredAvroData(parquetAvroAdTechProductCatalogSchema, input);

    GenericRecord expected = new GenericData.Record(parquetAvroAdTechProductCatalogSchema);
    expected.put("trackingeventid", "7ca521ae-ae7b-33a3-b876-738fc6719fdd");
    expected.put("scribelogid", "8b1d4bac-797e-4a5f-a959-667deda80515");
    expected.put("platform", "WEB");
    expected.put("storeid", 49);
    expected.put("eventtimestamp", "2017-05-15 06:13:32.576 -0400");
    expected.put("libraguid", "0ae406c8-5919-7c50-a927-7040475fd802");
    expected.put("optioncombinationid", 4075458971L);
    expected.put("sku", "SKU123");
    expected.put("brandcatalogid", 4075458971L);
    expected.put("source", "SOURCE");
    expected.put("displaysku", "sku876");
    expected.put("name", "nametest");
    expected.put("marketingcategory", "testCategory");
    expected.put("status", 4L);
    expected.put("statusflag", "statusTemp");
    expected.put("webdescription", "describe lklmk");
    expected.put("romancecopy", "romanceCopy");
    expected.put("reviewrating", 4.4);
    expected.put("reviewrating2", 3.9);
    expected.put("numberofratings", 100L);
    expected.put("salescount", 9988L);
    expected.put("currency", "dollars");
    expected.put("skumanufacturerpartnumber", "statusTemp");
    expected.put("whitelabelpartnumber", "describe lklmk");
    expected.put("isnew", true);
    expected.put("iskit", true);
    expected.put("isb2bonly", false);
    expected.put("isfindable", true);
    expected.put("isautocreate", false);
    expected.put("hasfreegroundshipping", false);
    // nested objects
    expected.put("masterclass", null);
    expected.put("productclasses", null);
    expected.put("options",  null);
    expected.put("productdimensions",  null);
    expected.put("manufacturer", expectedManufacturer);
    expected.put("manufacturerpart", expectedproductCatalogManufacturerPart);

    // exploded nested object
    expected.put("supplierparts",  null);
    expected.put("promotion",  null);
    expected.put("isadminonly", true);
    expected.put("prrestrictionsbitmap", null);
    expected.put("pwrbayesianrating", 4.3);
    expected.put("isswatchorderable", false);
    expected.put("dimensionalinfo", null);
    expected.put("proproduct", null);
    expected.put("exposurewarning", null);
    assertEquals(expected, actual);
  }
}