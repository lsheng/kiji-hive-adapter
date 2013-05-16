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

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.hive.udf.utils.ArgumentUtils;
import org.kiji.hive.utils.HiveTypes.HiveStruct;

/**
 * UDF that filters the results from KijiRowExpressions.  Returns the same type as what was
 * passed in, but with the entries that are outside of the range removed.
 *
 * Supported Kiji Row Expressions:
 * <ul>
 *   <li>family - MAP&gt;STRING, ARRAY&gt;STRUCT&gt;TIMESTAMP, cell&lt;&lt;&lt;
 *   <li>family:qualifier - ARRAY&gt;STRUCT&gt;TIMESTAMP, cell&lt;&lt;
 * </ul>
 *
 * To use this UDF in hive, a temporary function needs to be added:
 * create temporary function timestamp_filter as 'org.kiji.hive.udf.GenericUDFTimestampFilter';
 */
@Description(name = "timestamp_filter",
    value = "_FUNC_(array(start, end, kijiresults)) - "
        + "Filters the cell data in the input column for timestamps within the specified range.")
public class GenericUDFTimestampFilter extends GenericUDF {
  private static final Logger LOG = LoggerFactory.getLogger(GenericUDFTimestampFilter.class);

  private ObjectInspector[] mArgumentOIs;

  private Object mResult;

  // Argument index constants
  private static final Integer START_TIME_ARGUMENT = 0;
  private static final Integer END_TIME_ARGUMENT = 1;
  private static final Integer COLUMNS_ARGUMENT = 2;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    ObjectInspector returnOI;

    if (arguments.length != 3) {
      throw new UDFArgumentLengthException(
          "TIMESTAMP_FILTER(array(start, end, kijiresults)) needs exactly 3 arguments.");
    }

    ArgumentUtils.validateArgument(arguments, START_TIME_ARGUMENT, Category.PRIMITIVE);
    ArgumentUtils.validateArgument(arguments, END_TIME_ARGUMENT, Category.PRIMITIVE);

    Set<Category> columnTypes = Sets.newHashSet(Category.LIST, Category.MAP);
    ArgumentUtils.validateArgument(arguments, COLUMNS_ARGUMENT, columnTypes);

    mArgumentOIs = arguments;

    switch (arguments[COLUMNS_ARGUMENT].getCategory()) {
      case LIST:
        ObjectInspector listElementObjectInspector =
            ((ListObjectInspector) (arguments[COLUMNS_ARGUMENT])).getListElementObjectInspector();
        returnOI =
            ObjectInspectorFactory.getStandardListObjectInspector(listElementObjectInspector);
        break;

      case MAP:
        ObjectInspector mapKeyObjectInspector =
            ((MapObjectInspector) (arguments[COLUMNS_ARGUMENT])).getMapKeyObjectInspector();
        ObjectInspector mapValueObjectInspector =
            ((MapObjectInspector) (arguments[COLUMNS_ARGUMENT])).getMapValueObjectInspector();
        returnOI = ObjectInspectorFactory.getStandardMapObjectInspector(
            mapKeyObjectInspector,
            mapValueObjectInspector);
        break;

      default:
        throw new UDFArgumentException("Unsupported: " + arguments[COLUMNS_ARGUMENT].getCategory());
    }

    return returnOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[COLUMNS_ARGUMENT].get() == null) {
      return null;
    }

    Object columnData = arguments[COLUMNS_ARGUMENT].get();
    Long start = ArgumentUtils.parseTimestamp(arguments[START_TIME_ARGUMENT]);
    Long end = ArgumentUtils.parseTimestamp(arguments[END_TIME_ARGUMENT]);

    switch (mArgumentOIs[COLUMNS_ARGUMENT].getCategory()) {
      case LIST:
        ListObjectInspector arrayOI = (ListObjectInspector) mArgumentOIs[COLUMNS_ARGUMENT];
        List<Object> retArray = (List) arrayOI.getList(columnData);
        mResult = filterList(retArray, start, end);
        break;

      case MAP:
        MapObjectInspector mapOI = (MapObjectInspector) mArgumentOIs[COLUMNS_ARGUMENT];
        Map<Object, Object> retMap = (Map) mapOI.getMap(columnData);
        Map resultMap = Maps.newHashMap();
        for (Map.Entry<Object, Object> entry : retMap.entrySet()) {
          List<Object> values = (List) entry.getValue();
          resultMap.put(entry.getKey(), filterList(values, start, end));
        }
        mResult = resultMap;
        break;

      default:
        throw new UDFArgumentException("Unsupported evaluation category: "
            + mArgumentOIs[COLUMNS_ARGUMENT].getCategory());
    }
    return mResult;
  }

  @Override
  public String getDisplayString(String[] children) {
    StringBuilder sb = new StringBuilder();
    sb.append("timestamp_filter(");
    for (int i = 0; i < children.length; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append(children[i]);
    }
    sb.append(')');
    return sb.toString();
  }

  /**
   * Removes all objects from the input list whose timestamps are not within the range inclusive.
   * @param list List of HiveStruct objects to filter.
   * @param start of the range inclusive, or null to disable start checking
   * @param end of the range inclusive, or null to disable end checking
   * @return list of HiveStruct objects whose timestamps are within the range.
   */
  private List<Object> filterList(List<Object> list, Long start, Long end) {
    List<Object> filtered = Lists.newArrayList();
    for (Object o : list) {
      HiveStruct hs = (HiveStruct) o;
      if (hs.get(0) instanceof Timestamp) {
        Timestamp ts = (Timestamp) hs.get(0);
        if (isTimestampBetween(ts, start, end)) {
          filtered.add(hs);
        }
      } else {
        LOG.warn("HiveStruct is not properly formatted with a timestamp: " + hs.toString());
      }
    }
    return filtered;
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
