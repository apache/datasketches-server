/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.server;

public final class SketchConstants {
  // API call paths, relative to root
  public static final String UPDATE_PATH = "update";
  public static final String SERIALIZE_PATH = "serialize";
  public static final String STATUS_PATH = "status";
  public static final String QUERY_PATH = "query";
  public static final String MERGE_PATH = "merge";
  public static final String RESET_PATH = "reset";

  // JSON Query/Update/Merge Field Names
  public static final String QUERY_NAME_FIELD = "name";
  public static final String QUERY_FAMILY_FIELD = "family";
  public static final String QUERY_SKETCH_FIELD = "sketch";
  public static final String QUERY_DATA_FIELD = "data";
  public static final String QUERY_PAIR_ITEM_FIELD = "item";
  public static final String QUERY_PAIR_WEIGHT_FIELD = "weight";
  public static final String QUERY_MERGE_TGT_FIELD = "target";
  public static final String QUERY_MERGE_SRC_FIELD = "source";
  public static final String QUERY_MERGE_K_FIELD = "k";
  public static final String QUERY_SUMMARY_FIELD = "summary";
  public static final String QUERY_ERRORTYPE_FIELD = "errorType";
  public static final String QUERY_ERRORTYPE_NO_FP = "noFalsePositives";
  public static final String QUERY_ERRORTYPE_NO_FN = "noFalseNegatives";
  public static final String QUERY_VALUES_FIELD_NAME = "values";
  public static final String QUERY_FRACTIONS_NAME_FIELD = "fractions";
  public static final String QUERY_RESULT_TYPE_NAME_FIELD = "resultType";
  public static final String QUERY_RESULT_TYPE_PMF = "pmf";
  public static final String QUERY_RESULT_TYPE_CDF = "cdf";

  // JSON Query Response Field Names
  public static final String RESPONSE_SUMMARY_FIELD = QUERY_SUMMARY_FIELD;
  public static final String RESPONSE_ESTIMATE_FIELD = "estimate";
  public static final String RESPONSE_ESTIMATION_MODE_FIELD = "estimationMode";
  public static final String RESPONSE_P1STDEV_FIELD = "plus1StdDev";
  public static final String RESPONSE_P2STDEV_FIELD = "plus2StdDev";
  public static final String RESPONSE_P3STDEV_FIELD = "plus3StdDev";
  public static final String RESPONSE_M1STDEV_FIELD = "minus1StdDev";
  public static final String RESPONSE_M2STDEV_FIELD = "minus2StdDev";
  public static final String RESPONSE_M3STDEV_FIELD = "minus3StdDev";
  public static final String RESPONSE_ITEMS_ARRAY = "items";
  public static final String RESPONSE_ITEM_VALUE = "item";
  public static final String RESPONSE_ITEM_ESTIMATE = "estimate";
  public static final String RESPONSE_ITEM_UPPER_BOUND = "upperBound";
  public static final String RESPONSE_ITEM_LOWER_BOUND = "lowerBound";
  public static final String RESPONSE_STREAM_WEIGHT = "streamWeight"; // used for sampling
  public static final String RESPONSE_SKETCH_K = "sketchK";
  public static final String RESPONSE_ITEM_WEIGHT = "weight";
  public static final String RESPONSE_STREAM_LENGTH = "streamLength";
  public static final String RESPONSE_MAX_VALUE = "maxValue";
  public static final String RESPONSE_MIN_VALUE = "minValue";
  public static final String RESPONSE_CDF_LIST = "estimatedCDF";
  public static final String RESPONSE_PMF_LIST = "estimatedPMF";
  public static final String RESPONSE_RESULT_VALUE = "value";
  public static final String RESPONSE_RESULT_RANK = "rank";
  public static final String RESPONSE_RESULT_MASS = "mass";
  public static final String RESPONSE_QUANTILE_LIST = "estimatedQuantiles";
  public static final String RESPONSE_RESULT_QUANTILE = "quantile";
  public static final String RESPONSE_SKETCH_COUNT_FIELD = "count";

  // JSON Config Field Names
  public static final String CONFIG_PORT_FIELD = "port";
  public static final String CONFIG_SKETCHES_PREFIX = "sketches"; // >= 1 fully described sketches
  public static final String CONFIG_SET_PREFIX = "set";
  public static final String CONFIG_K_FIELD = "k";
  public static final String CONFIG_FAMILY_FIELD = "family";
  public static final String CONFIG_TYPE_FIELD = "type"; // value type, only for distinct counting
  public static final String CONFIG_SKETCH_NAME_FIELD = "name";
  public static final String CONFIG_SET_NAMES_FIELD = "names";

  // JSON Sketch Types
  public static final String SKETCH_FAMILY_THETA = "theta";
  public static final String SKETCH_FAMILY_HLL = "hll";
  public static final String SKETCH_FAMILY_CPC = "cpc";
  public static final String SKETCH_FAMILY_FREQUENCY = "frequency";
  public static final String SKETCH_FAMILY_KLL = "kll";
  public static final String SKETCH_FAMILY_RESERVOIR = "reservoir";
  public static final String SKETCH_FAMILY_VAROPT = "varopt";

  // JSON Value Types (applicable only for distinct counting)
  public static final String VALUE_TYPE_INT = "int";
  public static final String VALUE_TYPE_LONG = "long";
  public static final String VALUE_TYPE_FLOAT = "float";
  public static final String VALUE_TYPE_DOUBLE = "double";
  public static final String VALUE_TYPE_STRING = "string";

  // server configuration
  public static final int DEFAULT_PORT = 8080;

  public static final String ERROR_KEY = "error";
}
