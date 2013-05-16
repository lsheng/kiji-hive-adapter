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

package org.kiji.hive.udf.utils;

import java.util.Collection;
import java.util.Collections;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Static helper methods for argument parsing within Kiji Hive Adapter UDFs.
 */
public final class ArgumentUtils {

  /** Utility class cannot be instantiated. */
  private ArgumentUtils() {}

  /**
   * Validates that an argument has correct category.
   *
   * @param arguments passed in arguments
   * @param argumentNumber the argument number to validate
   * @param category the allowed category
   * @throws UDFArgumentTypeException if there is an error
   */
  public static void validateArgument(ObjectInspector[] arguments,
      Integer argumentNumber,
      Category category)
      throws UDFArgumentTypeException {
    validateArgument(arguments, argumentNumber, Collections.singleton(category));
  }

  /**
   * Validates that an argument has correct category.
   *
   * @param arguments passed in arguments
   * @param argumentNumber the argument number to validate
   * @param categories the allowed categories
   * @throws UDFArgumentTypeException if there is an error
   */
  public static void validateArgument(ObjectInspector[] arguments,
      Integer argumentNumber,
      Collection<Category> categories)
      throws UDFArgumentTypeException {
    if (!categories.contains(arguments[argumentNumber].getCategory()))  {
      throw new UDFArgumentTypeException(argumentNumber,
          "Argument " + argumentNumber
          + " of function TIMESTAMP_FILTER must be in " + categories.toString()
          + ", but " + arguments[argumentNumber].getTypeName()
          + " was found.");
    }

  }

  /**
   * Parses a passed in timestamp.  Can return null if null is passed in.  Will automatically
   * retrieve the object contained in a DeferredObject if necessary.
   *
   * @param timestampObj a Object containing a timestamp argument.
   * @return a Long timestamp value, or null if null is passed in.
   * @throws HiveException if there was an issue with obtaining the decoded object.
   */
  public static Long parseTimestamp(Object timestampObj)
      throws HiveException {

    Object ts;
    if (timestampObj instanceof GenericUDF.DeferredObject) {
      ts = ((GenericUDF.DeferredObject) timestampObj).get();
    } else {
      ts = timestampObj;
    }

    Long timestamp;
    if (ts == null) {
      timestamp = null;
    } else if (ts instanceof LongWritable) {
      timestamp = ((LongWritable) ts).get();
    } else if (ts instanceof IntWritable) {
      timestamp = (long) ((IntWritable) ts).get();
    } else {
      throw new UDFArgumentException("Timestamp argument must be an int or a long.");
    }
    return timestamp;
  }

}
