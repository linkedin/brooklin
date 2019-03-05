/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.avrogenerator;

import java.sql.SQLException;

import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class TestOracleDatabaseClient {
  private static final String JOBS_SCHEMA = "JOBS";
  private static final String JOBS_TABLE = "JOBS";

  @Test(enabled = false)
  public void testBasic() throws SQLException {
    String conUri = "jdbc:oracle:thin:jobs/jobs8uat@//lca1-eitm-jobs.stg.linkedin.com:1521/EI_EITM_JOBS";
    OracleDatabaseClient client = new OracleDatabaseClient(conUri);
    client.initializeConnection();

    boolean b = client.isTable(JOBS_SCHEMA, JOBS_TABLE);
    Assert.assertEquals(b, true);

    b = client.isPrimitive("VARCHAR2");
    Assert.assertEquals(b, true);

    b = client.isPrimitive("NUMBER");
    Assert.assertEquals(b, true);

    b = client.isPrimitive("DATABUS_GEO_T");
    Assert.assertEquals(b, false);

    b = client.isStruct(JOBS_SCHEMA, "DATABUS_GEO_T");
    Assert.assertEquals(b, true);

    b = client.isCollection(JOBS_SCHEMA, "DATABUS_COMPANY_ARRAY_T");
    Assert.assertEquals(b, true);

    OracleDatabaseClient.CollectionMetadata metadata = client.getCollectionMetadata(JOBS_SCHEMA, "DATABUS_COMPANY_ARRAY_T");
    Assert.assertEquals("DATABUS_COMPANY_T", metadata.getElementFieldTypeName());
    Assert.assertEquals(metadata.getElementPrecision(), 0);
    Assert.assertEquals(metadata.getElementScale(), 0);
    Assert.assertEquals(metadata.getElementSchemaName(), "JOBS");

    client.closeConnection();
  }
}
