package com.linkedin.datastream.server.dms;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.server.EmbeddedDatastreamCluster;
import com.linkedin.datastream.server.TestDatastreamServer;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.PagingContext;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Test DatastreamResources with zookeeper backed DatastreamStore
 */
@Test(singleThreaded = true)
public class TestDatastreamResources {

  private static final PagingContext NO_PAGING = new PagingContext(0, 0, false, false);

  private EmbeddedDatastreamCluster _datastreamKafkaCluster;

  public static Datastream generateDatastream(int seed) {
    return generateDatastream(seed, new HashSet<>());
  }

  public static Datastream generateDatastream(int seed, Set<String> missingFields) {
    Datastream ds = new Datastream();
    if (!missingFields.contains("name")) {
      ds.setName("name_" + seed);
    }
    if (!missingFields.contains("connectorType")) {
      ds.setConnectorType(DummyConnector.CONNECTOR_TYPE);
    }
    if (!missingFields.contains("source")) {
      ds.setSource(new DatastreamSource());
      ds.getSource().setConnectionString(DummyConnector.VALID_DUMMY_SOURCE);
    }
    if (!missingFields.contains("target")) {
      String metadataBrokers = "kafkaBrokers_" + seed;
      String targetTopic = "kafkaTopic_" + seed;
    }
    if (!missingFields.contains("metadata")) {
      StringMap metadata = new StringMap();
      metadata.put("owner", "person_" + seed);
      ds.setMetadata(metadata);
    }
    return ds;
  }

  @BeforeMethod
  public void setUp() throws Exception {
    _datastreamKafkaCluster = TestDatastreamServer.initializeTestDatastreamServerWithDummyConnector(null);
    _datastreamKafkaCluster.startup();
  }

  @AfterMethod
  public void cleanup() {
    _datastreamKafkaCluster.shutdown();
  }

  @Test
  public void testReadDatastream() {
    DatastreamResources resource1 = new DatastreamResources(_datastreamKafkaCluster.getPrimaryDatastreamServer());
    DatastreamResources resource2 = new DatastreamResources(_datastreamKafkaCluster.getPrimaryDatastreamServer());

    // read before creating
    Datastream ds = resource1.get("name_0");
    Assert.assertNull(ds);

    Datastream datastreamToCreate = generateDatastream(0);
    CreateResponse response = resource1.create(datastreamToCreate);
    Assert.assertNull(response.getError());
    Assert.assertEquals(response.getStatus(), HttpStatus.S_201_CREATED);

    ds = resource2.get("name_0");
    Assert.assertNotNull(ds);
    datastreamToCreate.setDestination(new DatastreamDestination());
    Assert.assertEquals(ds, datastreamToCreate);
  }

  @Test
  public void testCreateDatastream() {
    DatastreamResources resource = new DatastreamResources(_datastreamKafkaCluster.getPrimaryDatastreamServer());
    Set<String> missingFields = new HashSet<>();

    // happy path
    Datastream fullDatastream = generateDatastream(1);
    CreateResponse response = resource.create(fullDatastream);
    Assert.assertNull(response.getError());
    Assert.assertEquals(response.getStatus(), HttpStatus.S_201_CREATED);

    missingFields.add("target");
    missingFields.add("metadata");
    Datastream allRequiredFields = generateDatastream(2, missingFields);
    response = resource.create(allRequiredFields);
    Assert.assertNull(response.getError());
    Assert.assertEquals(response.getStatus(), HttpStatus.S_201_CREATED);

    // missing necessary fields
    missingFields.clear();
    missingFields.add("name");
    Datastream noName = generateDatastream(3, missingFields);
    response = resource.create(noName);
    Assert.assertNotNull(response.getError());
    Assert.assertEquals(response.getError().getStatus(), HttpStatus.S_400_BAD_REQUEST);

    missingFields.clear();
    missingFields.add("connectorType");
    Datastream noConnectorType = generateDatastream(4, missingFields);
    response = resource.create(noConnectorType);
    Assert.assertNotNull(response.getError());
    Assert.assertEquals(response.getError().getStatus(), HttpStatus.S_400_BAD_REQUEST);

    missingFields.clear();
    missingFields.add("source");
    Datastream noSource = generateDatastream(5, missingFields);
    response = resource.create(noSource);
    Assert.assertNotNull(response.getError());
    Assert.assertEquals(response.getError().getStatus(), HttpStatus.S_400_BAD_REQUEST);

    // creating existing Datastream
    response = resource.create(allRequiredFields);
    Assert.assertNotNull(response.getError());
    Assert.assertEquals(response.getError().getStatus(), HttpStatus.S_409_CONFLICT);
  }

  @Test
  public void testCreateInvalidDatastream() {
    DatastreamResources resource = new DatastreamResources(_datastreamKafkaCluster.getPrimaryDatastreamServer());
    Datastream datastream1 = generateDatastream(6);
    datastream1.setConnectorType("InvalidConnectorName");
    CreateResponse response = resource.create(datastream1);
    Assert.assertNotNull(response.getError());
    Assert.assertEquals(response.getError().getStatus(), HttpStatus.S_400_BAD_REQUEST);

    Datastream datastream2 = generateDatastream(7);
    datastream2.setSource(new DatastreamSource());
    datastream2.getSource().setConnectionString("InvalidSource");
    CreateResponse response2 = resource.create(datastream1);
    Assert.assertNotNull(response2.getError());
    Assert.assertEquals(response2.getError().getStatus(), HttpStatus.S_400_BAD_REQUEST);
  }

  private Datastream createDatastream(DatastreamResources resource, String name, int seed) {
    Datastream stream = generateDatastream(seed, new HashSet<>(Arrays.asList("name")));
    stream.setName(name + seed);
    CreateResponse response = resource.create(stream);
    Assert.assertNull(response.getError());
    Assert.assertEquals(response.getStatus(), HttpStatus.S_201_CREATED);
    return stream;
  }

  private List<Datastream> createDataStreams(DatastreamResources resource, String preffix, int count)
      throws Exception {
    return IntStream.range(0, count).mapToObj(n -> createDatastream(resource, preffix, n)).collect(Collectors.toList());
  }

  @Test
  public void testCreateGetAllDatastreams() throws Exception {
    DatastreamResources resource = new DatastreamResources(_datastreamKafkaCluster.getPrimaryDatastreamServer());

    Assert.assertEquals(resource.getAll(NO_PAGING).size(), 0);

    String datastreamName = "TestDatastream-";
    List<Datastream> datastreams = createDataStreams(resource, datastreamName, 10);

    // Get All
    Optional<List<Datastream>> result =
        PollUtils.poll(() -> resource.getAll(NO_PAGING), streams -> streams.size() == datastreams.size(), 100, 1000);

    Assert.assertTrue(result.isPresent());

    List<Datastream> queryStreams = result.get();

    queryStreams.forEach(queryStream -> Assert.assertNotNull(queryStream.getDestination()));

    // Compare datastreams set only by name since destination is empty upon creation and later populated
    Assert.assertEquals(datastreams.stream().map(Datastream::getName).collect(Collectors.toSet()),
        queryStreams.stream().map(Datastream::getName).collect(Collectors.toSet()));

    // Delete one entry
    Datastream removed = queryStreams.remove(0);
    Assert.assertTrue(resource.delete(removed.getName()).getStatus() == HttpStatus.S_200_OK);

    // Get All
    List<Datastream> remainingQueryStreams = resource.getAll(NO_PAGING);

    // Compare datastreams set only by name since destination is empty upon creation and later populated
    Assert.assertEquals(queryStreams.stream().map(Datastream::getName).collect(Collectors.toSet()),
        remainingQueryStreams.stream().map(Datastream::getName).collect(Collectors.toSet()));
  }

  // This test is flaky, Need to deflake this before enabling the test.
  // This doesn't fail often, So need to run several times before you can catch the flakiness.
  @Test(enabled = false)
  public void testCreateGetAllDatastreamsPaging() throws Exception {
    DatastreamResources resource = new DatastreamResources(_datastreamKafkaCluster.getPrimaryDatastreamServer());

    Assert.assertEquals(resource.getAll(NO_PAGING).size(), 0);

    List<Datastream> datastreams = createDataStreams(resource, "TestDatastream-", 10);

    int skip = 2;
    int limit = 5;

    // Get All
    List<Datastream> queryStreams = resource.getAll(new PagingContext(skip, limit));
    queryStreams.forEach(queryStream -> Assert.assertNotNull(queryStream.getDestination()));

    // Compare datastreams set only by name since destination is empty upon creation and later populated
    Assert.assertEquals(
        datastreams.stream().map(Datastream::getName).skip(skip).limit(limit).collect(Collectors.toSet()),
        queryStreams.stream().map(Datastream::getName).collect(Collectors.toSet()));
  }
}
