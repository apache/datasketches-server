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

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesException;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.sampling.ReservoirItemsSketch;
import org.apache.datasketches.sampling.VarOptItemsSamples;
import org.apache.datasketches.sampling.VarOptItemsSketch;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.Union;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import static org.apache.datasketches.server.SketchConstants.*;

/**
 * Allows querying the sketches to obtain estimates of the relevant quantities.
 *
 * Full description to be added later.
 */
public class DataQueryHandler extends BaseSketchesQueryHandler {
  public DataQueryHandler(final SketchStorage sketches) {
    super(sketches, false);
  }

  @Override
  protected JsonObject processQuery(final JsonObject query) {
    if (!query.has(QUERY_NAME_FIELD)) {
      throw new IllegalArgumentException("Query missing sketch name field");
    }

    final String key = query.get(QUERY_NAME_FIELD).getAsString();
    if (!sketches.contains(key)) {
      throw new IllegalArgumentException("Invalid sketch name: " + key);
    }

    final SketchStorage.SketchEntry se = sketches.getSketch(key);
    final JsonObject result;

    switch (se.family) {
      case UNION:
      case HLL:
      case CPC:
        result = processDistinctQuery(query, se.family, se.sketch);
        break;

      case KLL:
        result = processQuantilesQuery(query, se.family, se.sketch);
        break;

      case FREQUENCY:
        result = processFrequencyQuery(query, se.family, se.sketch);
        break;

      case RESERVOIR:
      case VAROPT:
        result = processSamplingQuery(query, se.family, se.sketch);
        break;

      default:
        throw new IllegalStateException("Unexpected sketch family: " + se.family);
    }

    return result;
  }

  private static boolean checkSummaryFlag(final JsonObject query) {
    boolean addSummary = false;
    if (query != null && query.has(QUERY_SUMMARY_FIELD)) {
      addSummary = query.get(QUERY_SUMMARY_FIELD).getAsBoolean();
    }
    return addSummary;
  }

  private static JsonObject processDistinctQuery(final JsonObject query, final Family type, final Object sketch) {
    if (query == null || type == null || sketch == null) {
      return null;
    }

    // check if we need a summary
    final boolean addSummary = checkSummaryFlag(query);
    String summary = null;

    final double estimate;
    final boolean isEstimationMode;
    final double p1StdDev;
    final double p2StdDev;
    final double p3StdDev;
    final double m1StdDev;
    final double m2StdDev;
    final double m3StdDev;

    switch (type) {
      case UNION:
        final CompactSketch thetaSketch = ((Union) sketch).getResult();
        isEstimationMode = thetaSketch.isEstimationMode();
        estimate = thetaSketch.getEstimate();
        p1StdDev = thetaSketch.getUpperBound(1);
        p2StdDev = thetaSketch.getUpperBound(2);
        p3StdDev = thetaSketch.getUpperBound(3);
        m1StdDev = thetaSketch.getLowerBound(1);
        m2StdDev = thetaSketch.getLowerBound(2);
        m3StdDev = thetaSketch.getLowerBound(3);
        if (addSummary) { summary = thetaSketch.toString(); }
        break;

      case CPC:
        final CpcSketch cpcSketch = (CpcSketch) sketch;
        isEstimationMode = true; // no exact mode
        estimate = cpcSketch.getEstimate();
        p1StdDev = cpcSketch.getUpperBound(1);
        p2StdDev = cpcSketch.getUpperBound(2);
        p3StdDev = cpcSketch.getUpperBound(3);
        m1StdDev = cpcSketch.getLowerBound(1);
        m2StdDev = cpcSketch.getLowerBound(2);
        m3StdDev = cpcSketch.getLowerBound(3);
        if (addSummary) { summary = cpcSketch.toString(); }
        break;

      case HLL:
        final HllSketch hllSketch = (HllSketch) sketch;
        isEstimationMode = hllSketch.isEstimationMode();
        estimate = hllSketch.getEstimate();
        p1StdDev = hllSketch.getUpperBound(1);
        p2StdDev = hllSketch.getUpperBound(2);
        p3StdDev = hllSketch.getUpperBound(3);
        m1StdDev = hllSketch.getLowerBound(1);
        m2StdDev = hllSketch.getLowerBound(2);
        m3StdDev = hllSketch.getLowerBound(3);
        if (addSummary) { summary = hllSketch.toString(); }
        break;

      default:
        throw new IllegalArgumentException("Unknown distinct counting sketch type: " + type.toString());
    }

    final JsonObject result = new JsonObject();
    result.addProperty(RESPONSE_ESTIMATE_FIELD, estimate);
    result.addProperty(RESPONSE_ESTIMATION_MODE_FIELD, isEstimationMode);
    result.addProperty(RESPONSE_P1STDEV_FIELD, p1StdDev);
    result.addProperty(RESPONSE_P2STDEV_FIELD, p2StdDev);
    result.addProperty(RESPONSE_P3STDEV_FIELD, p3StdDev);
    result.addProperty(RESPONSE_M1STDEV_FIELD, m1StdDev);
    result.addProperty(RESPONSE_M2STDEV_FIELD, m2StdDev);
    result.addProperty(RESPONSE_M3STDEV_FIELD, m3StdDev);
    if (addSummary)
      result.addProperty(RESPONSE_SUMMARY_FIELD, summary);

    return result;
  }

  private static JsonObject processQuantilesQuery(final JsonObject query, final Family type, final Object sketch) {
    if (query == null || type == null || sketch == null) {
      return null;
    }

    // check if we need a summary
    final boolean addSummary = checkSummaryFlag(query);
    String summary = null;

    final boolean isEstimationMode;
    final float maxValue;
    final float minValue;
    final long streamLength;

    final double[] fractions = getFractionsArray(query);
    float[] quantiles = null;

    final float[] values = getValuesArray(query);
    final String resultType = getResultType(query);
    double[] ranks = null;

    // since we know REQ is coming
    switch (type) {
      case KLL:
        final KllFloatsSketch kll = (KllFloatsSketch) sketch;
        isEstimationMode = kll.isEstimationMode();
        maxValue = kll.getMaxValue();
        minValue = kll.getMinValue();
        streamLength = kll.getN();

        if (values != null) {
          ranks = resultType.equals(QUERY_RESULT_TYPE_CDF) ? kll.getCDF(values) : kll.getPMF(values);
        }

        if (fractions != null) {
          quantiles = kll.getQuantiles(fractions);
        }

        if (addSummary)
          summary = kll.toString();
        break;

      default:
        throw new SketchesException("processQuantilesQuery() received a non-quantiles sketch: " + type.toString());
    }

    final JsonObject result = new JsonObject();
    result.addProperty(RESPONSE_STREAM_LENGTH, streamLength);
    result.addProperty(RESPONSE_ESTIMATION_MODE_FIELD, isEstimationMode);
    result.addProperty(RESPONSE_MIN_VALUE, minValue);
    result.addProperty(RESPONSE_MAX_VALUE, maxValue);

    if (ranks != null) {
      final String label = resultType.equals(QUERY_RESULT_TYPE_PMF) ? RESPONSE_RESULT_MASS : RESPONSE_RESULT_RANK;
      final JsonArray rankArray = new JsonArray();
      for (int i = 0; i < ranks.length; ++i) {
        final JsonObject rankPair = new JsonObject();
        if (i == values.length) {
          rankPair.addProperty(RESPONSE_RESULT_VALUE, maxValue);
        } else {
          rankPair.addProperty(RESPONSE_RESULT_VALUE, values[i]);
        }
        rankPair.addProperty(label, ranks[i]);
        rankArray.add(rankPair);
      }

      if (resultType.equals(QUERY_RESULT_TYPE_CDF))
        result.add(RESPONSE_CDF_LIST, rankArray);
      else
        result.add(RESPONSE_PMF_LIST, rankArray);
    }

    if (quantiles != null) {
      final JsonArray quantileArray = new JsonArray();
      for (int i = 0; i < quantiles.length; ++i) {
        final JsonObject quantilePair = new JsonObject();
        quantilePair.addProperty(RESPONSE_RESULT_RANK, fractions[i]);
        quantilePair.addProperty(RESPONSE_RESULT_QUANTILE, quantiles[i]);
        quantileArray.add(quantilePair);
      }
      result.add(RESPONSE_QUANTILE_LIST, quantileArray);
    }

    if (addSummary)
      result.addProperty(RESPONSE_SUMMARY_FIELD, summary);

    return result;
  }

  // only one sketch type here so could use ItemsSketch<String> instead of Object, but
  // we'll eep the signatures generic here
  @SuppressWarnings("unchecked")
  private static JsonObject processFrequencyQuery(final JsonObject query, final Family type, final Object sketch) {
    if (query == null || type != Family.FREQUENCY || sketch == null) {
      return null;
    }

    final ItemsSketch<String> sk = (ItemsSketch<String>) sketch;

    // check if we need a summary
    final boolean addSummary = checkSummaryFlag(query);

    if (!query.has(QUERY_ERRORTYPE_FIELD)) {
      throw new SketchesException("Must specify a value for " + QUERY_ERRORTYPE_FIELD
          + " for Frequent Items queries");
    }

    final ErrorType errorType;
    final String errorTypeString = query.get(QUERY_ERRORTYPE_FIELD).getAsString();
    if (errorTypeString.equals(QUERY_ERRORTYPE_NO_FP)) {
      errorType = ErrorType.NO_FALSE_POSITIVES;
    } else if (errorTypeString.equals(QUERY_ERRORTYPE_NO_FN)) {
      errorType = ErrorType.NO_FALSE_NEGATIVES;
    } else {
      throw new SketchesException("Unknown Frequent Items ErrorType: " + errorTypeString);
    }

    final ItemsSketch.Row<String>[] items = sk.getFrequentItems(errorType);

    final JsonArray itemArray = new JsonArray();
    for (final ItemsSketch.Row<String> item : items) {
      final JsonObject row = new JsonObject();
      row.addProperty(RESPONSE_ITEM_VALUE, item.getItem());
      row.addProperty(RESPONSE_ITEM_ESTIMATE, item.getEstimate());
      row.addProperty(RESPONSE_ITEM_UPPER_BOUND, item.getUpperBound());
      row.addProperty(RESPONSE_ITEM_LOWER_BOUND, item.getLowerBound());
      itemArray.add(row);
    }

    final JsonObject result = new JsonObject();
    result.add(RESPONSE_ITEMS_ARRAY, itemArray);
    if (addSummary)
      result.addProperty(RESPONSE_SUMMARY_FIELD, sk.toString());

    return result;
  }

  @SuppressWarnings("unchecked")
  private static JsonObject processSamplingQuery(final JsonObject query, final Family type, final Object sketch) {
    if (query == null || type == null || sketch == null) {
      return null;
    }

    // check if we need a summary
    final boolean addSummary = checkSummaryFlag(query);
    String summary = null;

    final long streamWeight;
    final int k;
    final JsonArray itemArray = new JsonArray();

    switch (type) {
      case RESERVOIR:
        final ReservoirItemsSketch<String> ris = (ReservoirItemsSketch<String>) sketch;
        for (final String item : ris.getSamples()) {
          itemArray.add(item);
        }
        streamWeight = ris.getN();
        k = ris.getK();
        if (addSummary)
          summary = ris.toString();
        break;

      case VAROPT:
        final VarOptItemsSketch<String> vis = (VarOptItemsSketch<String>) sketch;
        for (final VarOptItemsSamples<String>.WeightedSample ws : vis.getSketchSamples()) {
          final JsonObject item = new JsonObject();
          item.addProperty(RESPONSE_ITEM_VALUE, ws.getItem());
          item.addProperty(RESPONSE_ITEM_WEIGHT, ws.getWeight());
          itemArray.add(item);
        }
        streamWeight = vis.getN();
        k = vis.getK();
        if (addSummary)
          summary = vis.toString();
        break;

      default:
        throw new SketchesException("processSamplingQuery() received a non-sampling sketch: " + type.toString());
    }

    final JsonObject result = new JsonObject();
    result.addProperty(RESPONSE_SKETCH_K, k);
    result.addProperty(RESPONSE_STREAM_WEIGHT, streamWeight);
    result.add(RESPONSE_ITEMS_ARRAY, itemArray);
    if (addSummary)
      result.addProperty(RESPONSE_SUMMARY_FIELD, summary);

    return result;
  }

  // returns an array of rank points, or null if none in query
  private static float[] getValuesArray(final JsonObject query) {
    if (query == null || !query.has(QUERY_VALUES_FIELD_NAME)) {
      return null;
    }

    final JsonArray valuesArray = query.get(QUERY_VALUES_FIELD_NAME).getAsJsonArray();
    final float[] values = new float[valuesArray.size()];

    for (int i = 0; i < values.length; ++i) {
      if (!valuesArray.get(i).isJsonPrimitive()) {
        throw new SketchesException("Invalid value in array. Must be a floating point value, found: " + valuesArray.get(i));
      }
      values[i] = valuesArray.get(i).getAsFloat();
    }

    return values;
  }

  // returns QUERY_RESULT_TYPE_PMF if specified in QUERY_RESULT_TYPE_NAME_FIELD, otherwise returns default of
  // QUERY_RESULT_TYPE_CDF
  private static String getResultType(final JsonObject query) {
    if (query == null || !query.has(QUERY_RESULT_TYPE_NAME_FIELD)) {
      return QUERY_RESULT_TYPE_CDF;
    }

    final JsonElement resultTypeElement = query.get(QUERY_RESULT_TYPE_NAME_FIELD);
    if (!resultTypeElement.isJsonPrimitive())
      return QUERY_RESULT_TYPE_CDF;

    final String resultType = resultTypeElement.getAsString().toLowerCase();
    if (resultType.equals(QUERY_RESULT_TYPE_PMF))
      return QUERY_RESULT_TYPE_PMF;
    else
      return QUERY_RESULT_TYPE_CDF;
  }

  // returns an array of provided split points, or null if none in query
  private static double[] getFractionsArray(final JsonObject query) {
    if (query == null || !query.has(QUERY_FRACTIONS_NAME_FIELD)) {
      return null;
    }

    final JsonArray fractionsArray = query.get(QUERY_FRACTIONS_NAME_FIELD).getAsJsonArray();
    final double[] fractions = new double[fractionsArray.size()];

    for (int i = 0; i < fractions.length; ++i) {
      if (!fractionsArray.get(i).isJsonPrimitive()) {
        throw new SketchesException("Invalid value in array. Must be float in [0.0, 1.0], found: " + fractionsArray.get(i));
      }
      fractions[i] = fractionsArray.get(i).getAsFloat();
    }

    return fractions;
  }

}
