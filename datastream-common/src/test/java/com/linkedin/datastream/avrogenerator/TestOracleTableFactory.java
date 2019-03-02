/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.avrogenerator;

import java.util.Collections;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class TestOracleTableFactory {
  @Test
  public void testE2EBuildFieldTypeFactory() throws Exception {
    OracleDatabaseClient client = Mockito.mock(OracleDatabaseClient.class);
    DatabaseSource.TableMetadata meta =
        new DatabaseSource.TableMetadata("CHAR", "name", DatabaseSource.TableMetadata.NULLABLE, 0, 0);

    Mockito.when(client.getTableMetadata("schema", "view")).thenReturn(Collections.singletonList(meta));
    Mockito.when(client.isPrimitive("CHAR")).thenReturn(true);

    OracleTableFactory factory = new OracleTableFactory(client);

    OracleTable table = factory.buildOracleTable("schema", "view", "key");
  }

  @Test
  public void testAbsentPrimaryKey() throws Exception {
    OracleDatabaseClient client = Mockito.mock(OracleDatabaseClient.class);
    DatabaseSource.TableMetadata meta =
        new DatabaseSource.TableMetadata("CHAR", "name", DatabaseSource.TableMetadata.NULLABLE, 0, 0);

    Mockito.when(client.getTableMetadata("schema", "view")).thenReturn(Collections.singletonList(meta));
    Mockito.when(client.isPrimitive("CHAR")).thenReturn(true);
    Mockito.when(client.getPrimaryKeyFields("view")).thenReturn(Collections.singletonList("pk_field"));

    OracleTableFactory factory = new OracleTableFactory(client);
    OracleTable table = factory.buildOracleTable("schema", "view", null);

    Assert.assertEquals(table.getPrimaryKey(), "pkField");
  }
}
