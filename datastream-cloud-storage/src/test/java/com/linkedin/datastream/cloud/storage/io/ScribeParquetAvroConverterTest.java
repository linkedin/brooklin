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

    Schema actual = ScribeParquetAvroConverter.generateParquetStructuredAvroSchema(landingSchema);

    Schema expected = new Schema.Parser().parse(
        Resources.toString(Resources.getResource("parquetavroschemas/LandingParquetAvroSchema.avsc"), StandardCharsets.UTF_8)
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
    Schema AdTectProductPricingSchema = schemaParser.parse(
        Resources.toString(Resources.getResource("avroschemas/AdTechProductPricingEvent.avsc"), StandardCharsets.UTF_8)
    );

    Schema actual = ScribeParquetAvroConverter.generateParquetStructuredAvroSchema(AdTectProductPricingSchema);

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

//  @Test
//  public void testSupplyChainItemRequestReturnRouteFoundRGenerateParquetStructuredAvroData() throws Exception {
//    Schema avroItemsRequestReturnRouteSchema = schemaParser.parse(
//        Resources.toString(Resources.getResource("avroschemas/SupplyChainItemsReturnRoutesFound.avsc"), StandardCharsets.UTF_8)
//    );
//
//    GenericRecord input = new GenericData.Record(avroItemsRequestReturnRouteSchema);
//
//    input.put("eventName", "ItemsReturnRoutesFound");
//    input.put("scribeEventId", "8b1d4bac-797e-4a5f-a959-667deda80515");
//    input.put("eventTimestamp", 1494843212576L);
//    input.put("eventTimestamp", 1494843212576L);
//
//    input.put("libraGuid", "0ae406c8-5919-7c50-a927-7040475fd802");
//    input.put("transactionId", "CuQGyFkZf0yoRHA+R4BzAg==");
//    input.put("referringTransactionId", "CuQGyHkZf0yoRHA+R4BzAg==");
//    input.put("datacenter", "test");
//    input.put("visitGuid", "01c3f8ef-316a-42ef-93cf-eda4296d6ee3");
//    input.put("deviceGuid", "0ae406c8-58de-49ce-9d4a-481a2a300902");
//
//
//  }


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
    expected.put("placementIds", Arrays.asList(12345678L, 87654321L));
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
      expectedOrderProduct.put("orderProductId", 56656L);
      expectedOrderProduct.put("quantity", 5L);
      expectedOrderProduct.put("discontinued", true);
      expectedOrderProduct.put("returnGroupId", 556L);
      expectedOrderProduct.put("deliveryStatus", "pending");
      // As we are testing with 1.0 events which doesn't have logical type in
      // there field schema, we are leaving the timestamp type as long itself.
      expectedOrderProduct.put("estimatedDeliveryDate", "2017-05-15 06:13:32.576 -0400");
    }

    GenericRecord expectedSplitOrderProduct = null;
    if (nestedParquetSplitOrderProduct != null) {
      expectedSplitOrderProduct = new GenericData.Record(parquetNested);
      expectedSplitOrderProduct.put("orderProductId", 556L);
      expectedSplitOrderProduct.put("quantity", 58L);
      expectedSplitOrderProduct.put("discontinued", false);
      expectedSplitOrderProduct.put("returnGroupId", 556L);
      expectedSplitOrderProduct.put("deliveryStatus", "pending");
      expectedSplitOrderProduct.put("estimatedDeliveryDate", "2017-05-15 06:13:32.576 -0400");
    }

    GenericRecord expectedCancelledClearanceOrderProduct = null;
    if (nestedParquetCancelledClearanceOrderProducts != null) {
      expectedCancelledClearanceOrderProduct = new GenericData.Record(parquetNested);
      expectedCancelledClearanceOrderProduct.put("orderProductId", 111556L);
      expectedCancelledClearanceOrderProduct.put("quantity", 9L);
      expectedCancelledClearanceOrderProduct.put("discontinued", true);
      expectedCancelledClearanceOrderProduct.put("returnGroupId", 556L);
      expectedCancelledClearanceOrderProduct.put("deliveryStatus", "pending");
      expectedCancelledClearanceOrderProduct.put("estimatedDeliveryDate", "2017-05-15 06:13:32.576 -0400");
    }

    List<GenericRecord> expectedOrderProductsList = new ArrayList<GenericRecord>();
    expectedOrderProductsList.add(expectedOrderProduct);

    List<GenericRecord> expectedSplitOrderProductList = new ArrayList<GenericRecord>();
    expectedSplitOrderProductList.add(expectedSplitOrderProduct);

    List<GenericRecord> expectedCancelledClearanceOrderProductList = new ArrayList<GenericRecord>();
    expectedCancelledClearanceOrderProductList.add(expectedCancelledClearanceOrderProduct);

    GenericRecord actual = ScribeParquetAvroConverter.generateParquetStructuredAvroData(parquetAvroOrderCancellationConfirmationSchema, input);

    GenericRecord expected = new GenericData.Record(parquetAvroOrderCancellationConfirmationSchema);
    expected.put("trackingEventId", "7ca521ae-ae7b-33a3-b876-738fc6719fdd");
    expected.put("scribeLogId", "8b1d4bac-797e-4a5f-a959-667deda80515");
    expected.put("platform", "WEB");
    expected.put("storeId", 49);
    expected.put("eventTimestamp", "2017-05-15 06:13:32.576 -0400");
    expected.put("libraGuid", "0ae406c8-5919-7c50-a927-7040475fd802");
    expected.put("customerId", 4075458971L);
    expected.put("orderId", 4075458971L);
    expected.put("emailAddress", "test@gmail.com");
    expected.put("orderCost", 128L);
    expected.put("webLinkCancelledOrderProducts", Arrays.asList(
        "XQT1565",
        "TOVF1019",
        "ANDV2130"
    ));
    expected.put("orderProducts", expectedOrderProductsList);
    expected.put("splitOrderProducts", expectedSplitOrderProductList);
    expected.put("cancelledClearanceOrderProducts", expectedCancelledClearanceOrderProductList);
    expected.put("cancelledReplacements", Arrays.asList(345, 123));
    expected.put("issueCredit", false);
    expected.put("originTimestamp", "2020-07-01 16:01:39.000 -0400");
    expected.put("staging", true);
    expected.put("notificationType", "Test123");


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
      expectedItem.put("salePriceInCents", 56656L);
      expectedItem.put("b2bPriceInCents", 5L);
      expectedItem.put("listPriceInCents", 56656L);
      expectedItem.put("clearancePriceInCents", 556L);
      expectedItem.put("msrpRrpPriceInCents", 56656L);
    }

    GenericRecord expectedMinimumOrderQuantity = null;
    if (nestedParquetMinimumOrderQuantity != null) {
      expectedMinimumOrderQuantity = new GenericData.Record(nestedParquetMinimumOrderQuantity);
      expectedMinimumOrderQuantity.put("salePriceInCents", 56656L);
      expectedMinimumOrderQuantity.put("b2bPriceInCents", 5L);
      expectedMinimumOrderQuantity.put("listPriceInCents", 56656L);
      expectedMinimumOrderQuantity.put("clearancePriceInCents", 556L);
      expectedMinimumOrderQuantity.put("msrpRrpPriceInCents", 56656L);
    }

    GenericRecord expectedUnit = null;
    if (nestedParquetUnit != null) {
      expectedUnit = new GenericData.Record(nestedParquetUnit);
      expectedUnit.put("salePriceInCents", 56656L);
      expectedUnit.put("b2bPriceInCents", 5L);
      expectedUnit.put("listPriceInCents", 56656L);
      expectedUnit.put("clearancePriceInCents", 556L);
      expectedUnit.put("msrpRrpPriceInCents", 56656L);
    }

    GenericRecord actual = ScribeParquetAvroConverter.generateParquetStructuredAvroData(parquetAvroAdTechProductPricingSchema, input);

    GenericRecord expected = new GenericData.Record(parquetAvroAdTechProductPricingSchema);
    expected.put("trackingEventId", "7ca521ae-ae7b-33a3-b876-738fc6719fdd");
    expected.put("scribeLogId", "8b1d4bac-797e-4a5f-a959-667deda80515");
    expected.put("platform", "WEB");
    expected.put("storeId", 49);
    expected.put("eventTimestamp", "2017-05-15 06:13:32.576 -0400");
    expected.put("libraGuid", "0ae406c8-5919-7c50-a927-7040475fd802");
    expected.put("optionCombinationId", 4075458971L);
    expected.put("sku", "SKU123");
    expected.put("brandCatalogId", 4075458971L);
    expected.put("source", "SOURCE");
    expected.put("isOnCloseOutSale", true);
    expected.put("isOnSale", false);
    expected.put("item", expectedItem);
    expected.put("minimumOrderQuantity", expectedMinimumOrderQuantity);
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
    //Schema nestedUpdatedRoute = getSchemaForNestedObjects(nestedAddedRoute, "updated_route");

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
//    Schema nestedscribeHeaderExpected = getSchemaForNestedObjects(parquetAvroItemsReturnRequestRouteSchema, "scribeHeader");
    Schema nestedUpdatedRouteExpected = getSchemaForNestedObjects(parquetAvroItemsReturnRequestRouteSchema, "addedRoutes");

    Schema nestedRouteExpected = getSchemaForNestedObjects(nestedUpdatedRouteExpected, "route");
    Schema nestedFinalRouteExpected = getSchemaForNestedObjects(parquetAvroItemsReturnRequestRouteSchema, "finalRoutes");
    GenericRecord routesExpected = null;
    if (nestedRouteExpected != null) {
      routesExpected = new GenericData.Record(nestedRouteExpected);
      routesExpected.put("destinationId", 90);
      routesExpected.put("destinationTypeId", 67);
      routesExpected.put("thirdPartyCarrierId", 14);
      routesExpected.put("rsId", 4);
    }


    GenericRecord updatedRoutesExpected = null;
    if (nestedUpdatedRouteExpected != null) {
      updatedRoutesExpected = new GenericData.Record(nestedUpdatedRouteExpected);
      updatedRoutesExpected.put("route", routesExpected);
      updatedRoutesExpected.put("reason", "reson");
    }


    List<GenericRecord> addedRoutesArrayExpected = new ArrayList<>();
    addedRoutesArrayExpected.add(updatedRoutesExpected);
    GenericArray<GenericRecord>  addedRoutesExpected = new GenericData.Array<>(Schema.createArray(parquetAvroItemsReturnRequestRouteSchema.getField("addedRoutes").schema()), addedRoutesArrayExpected);



    List<GenericRecord> filteredRoutesArrayExpected = new ArrayList<>();
    filteredRoutesArrayExpected.add(updatedRoutesExpected);
    GenericArray<GenericRecord>  filteredRoutesExpected = new GenericData.Array<>(Schema.createArray(parquetAvroItemsReturnRequestRouteSchema.getField("filteredRoutes").schema()), filteredRoutesArrayExpected);

    GenericRecord finalRoutesExpected = null;
    if (nestedFinalRouteExpected != null) {
      finalRoutesExpected = new GenericData.Record(nestedFinalRoute);
      finalRoutesExpected.put("route", routesInput);
      finalRoutesExpected.put("expectedNetRecovery", 3.4f);
      finalRoutesExpected.put("grossRecovery", 1.3f);
      finalRoutesExpected.put("cost", 389.67f);
      finalRoutesExpected.put("currency", "dollars");
      finalRoutesExpected.put("carrierShipCost", 10.99f);
      finalRoutesExpected.put("marketingCategoryId", 11);

      finalRoutesExpected.put("openBoxDiscount", 7.60f);
      finalRoutesExpected.put("liquidationTransferFee", 9f);
      finalRoutesExpected.put("gradeProbabilityNew", 11f);
      finalRoutesExpected.put("gradeProbabilityOpenBox", 9.67f);
      finalRoutesExpected.put("gradeProbabilityLiquidation", 2.3f);
      finalRoutesExpected.put("gradeProbabilityJunk", 1.99f);
      finalRoutesExpected.put("junkGradeDisposalFee", 1.1f);

      finalRoutesExpected.put("warehouseProcessingCost", 3.3f);
      finalRoutesExpected.put("liquidationDiscountRate", 10.99f);
      finalRoutesExpected.put("openBoxDiscount", 7.60f);
      finalRoutesExpected.put("liquidationTransferFee", 9f);
      finalRoutesExpected.put("wholesalePrice", 200f);
      finalRoutesExpected.put("salePrice", 299f);
      finalRoutesExpected.put("volumeSteeringConstant", 2.3f);
      finalRoutesExpected.put("boxCountMultiplier", 0.99f);
      finalRoutesExpected.put("productStatusMultiplier", 11f);


      finalRoutesExpected.put("adjustmentMultiplier", 7.60f);
      finalRoutesExpected.put("returnReasonMultiplier", 9f);
      finalRoutesExpected.put("highDamageMultiplier", 200f);
      finalRoutesExpected.put("repackMultiplier", 299f);
      finalRoutesExpected.put("disassemblyMultiplier", 2.3f);

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
    expected.put("scribeEventId", "7ca521ae-ae7b-33a3-b876-738fc6719fdd");
    expected.put("eventName", "item_return_routes_found");
    expected.put("eventTimestamp", "2017-05-15 06:13:32.576 -0400");
    expected.put("receivedTimestamp", "2017-05-15 06:13:32.579 -0400");
    expected.put("dataClassification", 1);
    expected.put("productApplicationToken", "token");
    expected.put("domain", "supplychain");
    expected.put("avroSchemaVersion", 2);
    expected.put("scribeClientVersion", "trail_1");

    expected.put("requestId", "test_id");
    expected.put("orderProductId", 1234L);
    expected.put("isIncident", false);
    expected.put("canItemBeReturnedToSupplier", true);
    expected.put("isFedexPickupAvailable", false);
    expected.put("isFedexLocationAvailable", true);
    expected.put("supplierTakeReturns", true);
    expected.put("supplierCountryId", 12);
    expected.put("supplierReturnCountryId", 34);
    expected.put("supplierAllowanceType", supplierAllowanceTypeExpected);
    expected.put("supplierReturnAuthorizationMethod", "method");
    expected.put("supplierCastleGateWarehouseId", 123);
    expected.put("isWayfairCarrier", true);
    expected.put("addedRoutes", addedRoutesExpected);
    expected.put("filteredRoutes", filteredRoutesExpected);
    expected.put("finalRoutes", finalRoutesArrayExpected);

    assertEquals(expected, actual);

  }
}