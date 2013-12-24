/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.hive;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.hive.io.KijiRowDataWritable;
import org.kiji.hive.utils.KijiDataRequestSerializer;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.schema.util.ResourceUtils;

public class TestKijiTableRecordReader extends KijiClientTest {
  private static final Long TIMESTAMP = 1L;
  private Kiji mKiji;
  private KijiTable mTable;
  private KijiTableReader mReader;

  @Before
  public void setupEnvironment() throws Exception {
    // Get the test table layouts.
    final KijiTableLayout layout = KijiTableLayout.newLayout(
        KijiTableLayouts.getLayout(KijiTableLayouts.PAGING_TEST));

    // Populate the environment.
    mKiji = new InstanceBuilder()
        .withTable("user", layout)
        .withRow("foo")
          .withFamily("info")
            .withQualifier("name").withValue(TIMESTAMP, "foo-val")
        .withRow("bar")
          .withFamily("info")
            .withQualifier("name").withValue(TIMESTAMP, "bar-val")
        .build();

    // Fill local variables.
    mTable = mKiji.openTable("user");
    mReader = mTable.openTableReader();
  }

  @After
  public void cleanupEnvironment() throws IOException {
    ResourceUtils.closeOrLog(mReader);
    ResourceUtils.releaseOrLog(mTable);
    ResourceUtils.releaseOrLog(mKiji);
  }

  @Test
  public void testFetchData() throws IOException {
    KijiURI kijiURI = mTable.getURI();
    byte[] startKey = new byte[0];
    byte[] endKey = new byte[0];
    KijiTableInputSplit tableInputSplit =
        new KijiTableInputSplit(kijiURI, startKey, endKey, null, null);

    Configuration conf = getConf();
    KijiDataRequest kijiDataRequest = KijiDataRequest.create("info", "name");
    conf.set("kiji.data.request", KijiDataRequestSerializer.serialize(kijiDataRequest));

    // Initialize KijiTableRecordReader
    KijiTableRecordReader tableRecordReader = new KijiTableRecordReader(tableInputSplit, conf);

    // Retrieve result
    ImmutableBytesWritable key = new ImmutableBytesWritable();
    KijiRowDataWritable value = new KijiRowDataWritable();
    int resultCount = 0;
    boolean hasResult = tableRecordReader.next(key, value);
    while (hasResult) {
      resultCount++;
      hasResult = tableRecordReader.next(key, value);
    }
    assertEquals(2, resultCount);
  }

  @Test
  public void testFetchPagedData() throws IOException {
    // Add some extra versions of the rows so that we can page through the results.
    KijiTableWriter kijiTableWriter = mTable.openTableWriter();
    EntityId entityId = mTable.getEntityId("foo");
    kijiTableWriter.put(entityId, "info", "name", TIMESTAMP + 1, "foo-val-update1");
    kijiTableWriter.put(entityId, "info", "name", TIMESTAMP + 2, "foo-val-update2");

    ResourceUtils.closeOrLog(kijiTableWriter);

    KijiURI kijiURI = mTable.getURI();
    byte[] startKey = new byte[0];
    byte[] endKey = new byte[0];
    KijiTableInputSplit tableInputSplit =
        new KijiTableInputSplit(kijiURI, startKey, endKey, null, null);

    Configuration conf = getConf();
    KijiDataRequest kijiDataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(10).withPageSize(1).add("info", "name"))
        .build();

    conf.set("kiji.data.request", KijiDataRequestSerializer.serialize(kijiDataRequest));

    // Initialize KijiTableRecordReader
    KijiTableRecordReader tableRecordReader = new KijiTableRecordReader(tableInputSplit, conf);

    // Retrieve result
    ImmutableBytesWritable key = new ImmutableBytesWritable();
    KijiRowDataWritable value = new KijiRowDataWritable();
    int resultCount = 0;
    boolean hasResult = tableRecordReader.next(key, value);
    while (hasResult) {
      resultCount++;
      hasResult = tableRecordReader.next(key, value);
    }

    // Should read 4 cells, 3 for foo, 1 for bar.
    assertEquals(4, resultCount);
    ResourceUtils.closeOrLog(tableRecordReader);
  }

  @Test
  public void testMultiplePagedColumns() throws IOException {
    // Add some extra versions of the rows so that we can page through the results.
    KijiTableWriter kijiTableWriter = mTable.openTableWriter();
    EntityId entityId = mTable.getEntityId("foo");
    kijiTableWriter.put(entityId, "info", "name", TIMESTAMP + 1, "foo-val-update1");
    kijiTableWriter.put(entityId, "info", "name", TIMESTAMP + 2, "foo-val-update2");
    kijiTableWriter.put(entityId, "info", "location", TIMESTAMP, "foo-location");
    kijiTableWriter.put(entityId, "info", "location", TIMESTAMP + 1, "foo-location-update1");
    kijiTableWriter.put(entityId, "info", "location", TIMESTAMP + 2, "foo-location-update2");
    kijiTableWriter.put(entityId, "info", "location", TIMESTAMP + 3, "foo-location-update3");
    ResourceUtils.closeOrLog(kijiTableWriter);

    KijiURI kijiURI = mTable.getURI();
    byte[] startKey = new byte[0];
    byte[] endKey = new byte[0];
    KijiTableInputSplit tableInputSplit =
        new KijiTableInputSplit(kijiURI, startKey, endKey, null, null);

    Configuration conf = getConf();
    KijiDataRequest kijiDataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(9).withPageSize(1).add("info", "name"))
        .addColumns(ColumnsDef.create().withMaxVersions(9).withPageSize(1).add("info", "location"))
        .build();

    conf.set("kiji.data.request", KijiDataRequestSerializer.serialize(kijiDataRequest));

    // Initialize KijiTableRecordReader
    KijiTableRecordReader tableRecordReader = new KijiTableRecordReader(tableInputSplit, conf);

    // Retrieve result
    ImmutableBytesWritable key = new ImmutableBytesWritable();
    KijiRowDataWritable value = new KijiRowDataWritable();
    int resultCount = 0;
    boolean hasResult = tableRecordReader.next(key, value);
    while (hasResult) {
      resultCount++;
      hasResult = tableRecordReader.next(key, value);
    }

    // Should read 4 cells, 3 for foo, 1 for bar.
    assertEquals(5, resultCount);
    ResourceUtils.closeOrLog(tableRecordReader);
  }
}
