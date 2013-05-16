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

package org.kiji.hive.udf;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.hive.KijiRowExpression;
import org.kiji.hive.TypeInfos;
import org.kiji.hive.utils.HiveTypes.HiveStruct;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.layout.KijiTableLayouts;

public class TestGenericUDFTimestampFilter extends KijiClientTest {
  private EntityId mEntityId;
  private Kiji mKiji;
  private KijiTable mTable;
  private KijiTableReader mReader;

  @Before
  public final void setupKijiInstance() throws IOException {
    // Sets up an instance with data at multiple timestamps to utilize the different row
    // expression types.
    mKiji = getKiji();
    mKiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.ROW_DATA_TEST));
    mTable = mKiji.openTable("table");
    mEntityId = mTable.getEntityId("row1");
    final KijiTableWriter writer = mTable.openTableWriter();
    try {
      // Map type column family
      writer.put(mEntityId, "map", "qualA", 10L, 5);
      writer.put(mEntityId, "map", "qualA", 20L, 6);
      writer.put(mEntityId, "map", "qualA", 30L, 7);
      writer.put(mEntityId, "map", "qualA", 40L, 8);
      writer.put(mEntityId, "map", "qualB", 40L, 7);

      // Regular column family
      writer.put(mEntityId, "family", "qual0", 10L, "a");
      writer.put(mEntityId, "family", "qual0", 20L, "b");
      writer.put(mEntityId, "family", "qual0", 30L, "c");
      writer.put(mEntityId, "family", "qual0", 40L, "d");
    } finally {
      writer.close();
    }
    mReader = mTable.openTableReader();
  }

  @After
  public final void teardownKijiInstance() throws IOException {
    mReader.close();
    mTable.release();
    mKiji.deleteTable("table");
  }

  @Test
  public void testFilterFamilyAllValues() throws IOException, HiveException {
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("map", TypeInfos.FAMILY_MAP_ALL_VALUES);

    // Build the evaluated result
    final KijiDataRequest kijiDataRequest = kijiRowExpression.getDataRequest();
    KijiRowData kijiRowData = mReader.get(mEntityId, kijiDataRequest);
    Object result = kijiRowExpression.evaluate(kijiRowData);

    // Make sure that we can filter to just the relevant items.
    Map<Object, List<List>> filteredResult = (Map) filter(35L, 45L, result);
    for (Entry<Object, List<List>> entry : filteredResult.entrySet()) {
      for (List values : entry.getValue()) {
        assertEquals(2, values.size());
        if (!values.isEmpty()) {
          assertEquals(new Date(40), values.get(0));
        }
      }
    }

    filteredResult = (Map) filter(25L, 35L, result);
    for (Entry<Object, List<List>> entry : filteredResult.entrySet()) {
      for (List values : entry.getValue()) {
        assertEquals(2, values.size());
        if (!values.isEmpty()) {
          assertEquals(new Date(30), values.get(0));
        }
      }
    }
  }

  @Test
  public void testFilterColumnAllValues() throws IOException, HiveException {
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("family:qual0", TypeInfos.COLUMN_ALL_VALUES);

    // Build the evaluated result
    final KijiDataRequest kijiDataRequest = kijiRowExpression.getDataRequest();
    KijiRowData kijiRowData = mReader.get(mEntityId, kijiDataRequest);
    Object result = kijiRowExpression.evaluate(kijiRowData);

    // Make sure that we can filter to just the relevant item.
    List filteredResult = (List) filter(15L, 25L, result);
    assertEquals(1, filteredResult.size());
    HiveStruct item = (HiveStruct) filteredResult.get(0);
    assertEquals(new Date(20), item.get(0));

    // Make sure that the timestamps are inclusive.
    filteredResult = (List) filter(30L, 30L, result);
    assertEquals(1, filteredResult.size());
    item = (HiveStruct) filteredResult.get(0);
    assertEquals(new Date(30), item.get(0));

    // Make sure that we can specify just a start timestamp.
    filteredResult = (List) filter(15L, null, result);
    assertEquals(3, filteredResult.size());

    // Make sure that we can specify just an end timestamp.
    filteredResult = (List) filter(null, 25L, result);
    assertEquals(2, filteredResult.size());

    // Make sure that we can specify no timestamps(and result in basically a no-op).
    filteredResult = (List) filter(null, null, result);
    assertEquals(4, filteredResult.size());
  }

  private Object filter(Long start, Long end, Object value) throws HiveException {
    Writable startWritable = null;
    if (start != null) {
      startWritable = new LongWritable(start);
    }

    Writable endWritable = null;
    if (end != null) {
      endWritable = new LongWritable(end);
    }

    ObjectInspector[] objectInspectors = new ObjectInspector[3];
    objectInspectors[0] =
        PrimitiveObjectInspectorFactory.javaLongObjectInspector;
    objectInspectors[1] =
        PrimitiveObjectInspectorFactory.javaLongObjectInspector;
    if (value instanceof List) {
      objectInspectors[2] = (ObjectInspector)
          ObjectInspectorFactory.getStandardListObjectInspector(null);
    } else if (value instanceof Map) {
      objectInspectors[2] = (ObjectInspector)
          ObjectInspectorFactory.getStandardConstantMapObjectInspector(null, null, null);
    } else {
      throw new HiveException("Unsupported value type: " + value.getClass());
    }

    GenericUDFTimestampFilter timeStampFilter = new GenericUDFTimestampFilter();
    timeStampFilter.initialize(objectInspectors);

    DeferredJavaObject[] arguments = new DeferredJavaObject[3];
    arguments[0] = new DeferredJavaObject(startWritable);
    arguments[1] = new DeferredJavaObject(endWritable);
    arguments[2] = new DeferredJavaObject(value);

    Object result = timeStampFilter.evaluate(arguments);
    return result;
  }
}

