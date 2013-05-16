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

import static org.kiji.hive.utils.HiveTypes.HiveStruct;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.TaskExecutionException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.hive.udf.utils.ArgumentUtils;

/**
 * GenericUDTFGetLatestState.
 *
 * Based on the data returned specified by the row expression, this has different meanings:
 * <li>family - returns a
 * <li>family[0] - returns a
 * <li>family:qualifier - Returns the most recent STRUCT<TIMESTAMP, cell> within the passed in
 *     ARRAY<STRUCT<TIMESTAMP, cell>> or null if none is found.
 * <li>family:qualifier[0] - Returns the STRUCT<TIMESTAMP, cell> if the timestamp is within the
 *     range or null otherwise.
 *
 * To use this UDF in hive, a temporary function needs to be added:
 * create temporary function gls as 'org.kiji.hive.udf.GenericUDTFGetLatestState';
 */
@Description(name = "getLatestState",
    value = "_FUNC_(a) - separates the elements of array a into multiple rows,"
        + " or the elements of a map into multiple rows and columns ")
public class GenericUDTFGetLatestState extends GenericUDTF {
  private static final Logger LOG = LoggerFactory.getLogger(GenericUDTFGetLatestState.class);

  private ObjectInspector inputOI = null;

  // Argument index constants
  private static final Integer START_TIME_ARGUMENT = 0;
  private static final Integer END_TIME_ARGUMENT = 1;
  private static final Integer COLUMN_ARGUMENTS_START = 2;

  @Override
  public void close() throws HiveException {
  }

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    if (args.length <= COLUMN_ARGUMENTS_START) {
      throw new UDFArgumentException("getLatestState() requires at least 3 arguments");
    }

    ArgumentUtils.validateArgument(args, START_TIME_ARGUMENT, Category.PRIMITIVE);
    ArgumentUtils.validateArgument(args, END_TIME_ARGUMENT, Category.PRIMITIVE);

    //FIXME Maybe need to add Category.MAP
    Set<Category> columnTypes = Sets.newHashSet(Category.LIST);
    for (int pos=COLUMN_ARGUMENTS_START; pos<args.length; pos++) {
      ArgumentUtils.validateArgument(args, pos, columnTypes);
    }

    ArrayList<String> fieldNames = new ArrayList<String>();
    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

    for (int pos=COLUMN_ARGUMENTS_START; pos<args.length; pos++) {
      fieldNames.add("col" + (pos-COLUMN_ARGUMENTS_START));
      switch (args[COLUMN_ARGUMENTS_START].getCategory()) {
        case LIST:
          inputOI = args[COLUMN_ARGUMENTS_START];
          //fieldNames.add("col");
          fieldOIs.add(((ListObjectInspector)inputOI).getListElementObjectInspector());
          break;
        case MAP:
          //FIXME this shouldn't be hittable
          inputOI = args[COLUMN_ARGUMENTS_START];
          fieldNames.add("key");
          fieldNames.add("value");
          fieldOIs.add(((MapObjectInspector)inputOI).getMapKeyObjectInspector());
          fieldOIs.add(((MapObjectInspector)inputOI).getMapValueObjectInspector());
          break;
        default:
          throw new UDFArgumentException("explode() takes an array or a map as a parameter");
      }
    }

    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
        fieldOIs);
  }

  private final Object[] forwardListObj = new Object[1];
  private final Object[] forwardMapObj = new Object[2];

  @Override
  public void process(Object[] args) throws HiveException {
    Long startArg = ArgumentUtils.parseTimestamp(args[START_TIME_ARGUMENT]);
    Long endArg = ArgumentUtils.parseTimestamp(args[END_TIME_ARGUMENT]);

    switch (inputOI.getCategory()) {
      case LIST:
        ListObjectInspector listOI = (ListObjectInspector)inputOI;
        List<?> list = listOI.getList(args[COLUMN_ARGUMENTS_START]);
        if (list == null) {
          return;
        }

        HiveStruct latest = null;
        Long start = startArg;

        for (Object r : list) {
          //FIXME need more error validation here.
          HiveStruct hs = (HiveStruct) r;
          Timestamp ts = (Timestamp) hs.get(0);
          if (isTimestampBetween(ts, start, endArg)) {
            // Update the most recent thing that we've found and ratchet up the minimum timestamp.
            latest = hs;
            start = ts.getTime();
          }
        }
        forwardListObj[0] = latest;
        forward(forwardListObj);
        break;
      case MAP:
        //FIXME make work
        MapObjectInspector mapOI = (MapObjectInspector)inputOI;
        Map<?,?> map = mapOI.getMap(args[COLUMN_ARGUMENTS_START]);
        if (map == null) {
          return;
        }
        for (Map.Entry<?,?> r : map.entrySet()) {
          forwardMapObj[0] = r.getKey();
          forwardMapObj[1] = r.getValue();
          forward(forwardMapObj);
        }
        break;
      default:
        throw new TaskExecutionException("getLatestState() can only operate on an array.");
    }
  }

  @Override
  public String toString() {
    return "getLatestState";
  }

  /**
   * Returns whether the specified timestamp is within the range inclusive.
   * @param ts timestamp to validate
   * @param start of the range inclusive, or null to disable start checking
   * @param end of the range inclusive, or null to disable end checking
   * @return whether the specified timestamp is within the inclusive range .
   */
  private boolean isTimestampBetween(Timestamp ts, Long start, Long end) {
    if (start == null || ts.getTime() >= start) {
      if (end == null || ts.getTime() <= end) {
        return true;
      }
    }
    return false;
  }
}

