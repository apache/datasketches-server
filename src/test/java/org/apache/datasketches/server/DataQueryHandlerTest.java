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

import static org.apache.datasketches.server.SketchConstants.QUERY_CDF_VALUES_FIELD_NAME;
import static org.apache.datasketches.server.SketchConstants.QUERY_ERRORTYPE_FIELD;
import static org.apache.datasketches.server.SketchConstants.QUERY_ERRORTYPE_NO_FN;
import static org.apache.datasketches.server.SketchConstants.QUERY_ERRORTYPE_NO_FP;
import static org.apache.datasketches.server.SketchConstants.QUERY_FRACTIONS_NAME_FIELD;
import static org.apache.datasketches.server.SketchConstants.QUERY_NAME_FIELD;
import static org.apache.datasketches.server.SketchConstants.QUERY_PATH;
import static org.apache.datasketches.server.SketchConstants.QUERY_PMF_VALUES_FIELD_NAME;
import static org.apache.datasketches.server.SketchConstants.QUERY_SUMMARY_FIELD;
import static org.apache.datasketches.server.SketchConstants.RESPONSE_CDF_LIST;
import static org.apache.datasketches.server.SketchConstants.RESPONSE_ESTIMATE_FIELD;
import static org.apache.datasketches.server.SketchConstants.RESPONSE_ESTIMATION_MODE_FIELD;
import static org.apache.datasketches.server.SketchConstants.RESPONSE_ITEMS_ARRAY;
import static org.apache.datasketches.server.SketchConstants.RESPONSE_ITEM_ESTIMATE;
import static org.apache.datasketches.server.SketchConstants.RESPONSE_ITEM_LOWER_BOUND;
import static org.apache.datasketches.server.SketchConstants.RESPONSE_ITEM_UPPER_BOUND;
import static org.apache.datasketches.server.SketchConstants.RESPONSE_ITEM_VALUE;
import static org.apache.datasketches.server.SketchConstants.RESPONSE_ITEM_WEIGHT;
import static org.apache.datasketches.server.SketchConstants.RESPONSE_M1STDEV_FIELD;
import static org.apache.datasketches.server.SketchConstants.RESPONSE_M2STDEV_FIELD;
import static org.apache.datasketches.server.SketchConstants.RESPONSE_M3STDEV_FIELD;
import static org.apache.datasketches.server.SketchConstants.RESPONSE_MAX_VALUE;
import static org.apache.datasketches.server.SketchConstants.RESPONSE_MIN_VALUE;
import static org.apache.datasketches.server.SketchConstants.RESPONSE_P1STDEV_FIELD;
import static org.apache.datasketches.server.SketchConstants.RESPONSE_P2STDEV_FIELD;
import static org.apache.datasketches.server.SketchConstants.RESPONSE_P3STDEV_FIELD;
import static org.apache.datasketches.server.SketchConstants.RESPONSE_PMF_LIST;
import static org.apache.datasketches.server.SketchConstants.RESPONSE_QUANTILE_LIST;
import static org.apache.datasketches.server.SketchConstants.RESPONSE_RESULT_MASS;
import static org.apache.datasketches.server.SketchConstants.RESPONSE_RESULT_QUANTILE;
import static org.apache.datasketches.server.SketchConstants.RESPONSE_RESULT_RANK;
import static org.apache.datasketches.server.SketchConstants.RESPONSE_RESULT_VALUE;
import static org.apache.datasketches.server.SketchConstants.RESPONSE_SKETCH_K;
import static org.apache.datasketches.server.SketchConstants.RESPONSE_STREAM_LENGTH;
import static org.apache.datasketches.server.SketchConstants.RESPONSE_SUMMARY_FIELD;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.sampling.ReservoirItemsSketch;
import org.apache.datasketches.sampling.VarOptItemsSketch;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.Union;
import org.eclipse.jetty.http.HttpStatus;
import org.testng.annotations.Test;


import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class DataQueryHandlerTest extends ServerTestBase {
  @Test
  public void emptyQuery() {
    final JsonObject response = new JsonObject();
    JsonObject request = new JsonObject();

    // completely empty request cannot be handled
    assertEquals(postData(QUERY_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);

    // invalid name field with real sketch name
    request.addProperty("notAName", "theta0");
    assertEquals(postData(QUERY_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);

    // valid name, empty value
    request = new JsonObject();
    request.add(QUERY_NAME_FIELD, new JsonObject());
    assertEquals(postData(QUERY_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);

    // valid name, invalid sketch name
    request = new JsonObject();
    request.addProperty(QUERY_NAME_FIELD, "sketchDoesNotExist");
    assertEquals(postData(QUERY_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);
  }

  @Test
  public void cpcQuery() {
    final String sketchName = "cpcOfLongs";
    final JsonObject response = new JsonObject();
    final JsonObject request = new JsonObject();

    // add data directly to sketch, ensure it exists
    CpcSketch sk = (CpcSketch) server_.getSketch(sketchName).sketch_;
    for (int i = 0; i < 5; ++i) { sk.update(i); }
    assertFalse(sk.isEmpty());

    request.addProperty(QUERY_NAME_FIELD, sketchName);
    request.addProperty(QUERY_SUMMARY_FIELD, true);
    assertEquals(getData(QUERY_PATH, request, response), HttpStatus.OK_200);

    // get sketch again before testing
    sk = (CpcSketch) server_.getSketch(sketchName).sketch_;
    final JsonObject queryData = response.getAsJsonObject(RESPONSE_FIELD);
    assertEquals(queryData.get(QUERY_NAME_FIELD).getAsString(), sketchName);
    // JSON does not guarantee full double precision so allow tolerance
    assertEquals(queryData.get(RESPONSE_ESTIMATE_FIELD).getAsDouble(), sk.getEstimate());
    assertTrue(queryData.get(RESPONSE_ESTIMATION_MODE_FIELD).getAsBoolean());
    assertEquals(queryData.get(RESPONSE_P1STDEV_FIELD).getAsDouble(), sk.getUpperBound(1));
    assertEquals(queryData.get(RESPONSE_P2STDEV_FIELD).getAsDouble(), sk.getUpperBound(2));
    assertEquals(queryData.get(RESPONSE_P3STDEV_FIELD).getAsDouble(), sk.getUpperBound(3));
    assertEquals(queryData.get(RESPONSE_M1STDEV_FIELD).getAsDouble(), sk.getLowerBound(1));
    assertEquals(queryData.get(RESPONSE_M2STDEV_FIELD).getAsDouble(), sk.getLowerBound(2));
    assertEquals(queryData.get(RESPONSE_M3STDEV_FIELD).getAsDouble(), sk.getLowerBound(3));
    assertTrue(queryData.has(RESPONSE_SUMMARY_FIELD));
    assertFalse(queryData.get(RESPONSE_SUMMARY_FIELD).getAsString().isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void fiQuery() {
    final String sketchName = "topItems";
    final JsonObject response = new JsonObject();
    final JsonObject request = new JsonObject();

    // add data directly to sketch, with a significant weight difference
    // and enough items that we should trigger a purge
    final ItemsSketch<String> sk = (ItemsSketch<String>) server_.getSketch(sketchName).sketch_;
    final int n = 8;
    for (int i = 0; i < n; ++i)
      sk.update(Integer.toString(i), Math.round(Math.pow(2, n - i)));

    // missing errorType
    request.addProperty(QUERY_NAME_FIELD, sketchName);
    assertEquals(getData(QUERY_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);

    // invalid errorType
    request.addProperty(QUERY_ERRORTYPE_FIELD, "invalid");
    assertEquals(getData(QUERY_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);

    // no false positives, no summary
    request.addProperty(QUERY_ERRORTYPE_FIELD, QUERY_ERRORTYPE_NO_FP);
    assertEquals(getData(QUERY_PATH, request, response), HttpStatus.OK_200);
    JsonObject responseData = response.getAsJsonObject(RESPONSE_FIELD);
    assertTrue(responseData.has(RESPONSE_ITEMS_ARRAY));
    final int numNoFPItems = responseData.get(RESPONSE_ITEMS_ARRAY).getAsJsonArray().size();
    for (final JsonElement elmt : responseData.get(RESPONSE_ITEMS_ARRAY).getAsJsonArray()) {
      final JsonObject item = elmt.getAsJsonObject();
      assertTrue(item.has(RESPONSE_ITEM_VALUE));
      assertTrue(item.has(RESPONSE_ITEM_ESTIMATE));
      assertTrue(item.has(RESPONSE_ITEM_UPPER_BOUND));
      assertTrue(item.has(RESPONSE_ITEM_LOWER_BOUND));
    }

    // no false negatives, with summary
    request.addProperty(QUERY_ERRORTYPE_FIELD, QUERY_ERRORTYPE_NO_FN);
    request.addProperty(QUERY_SUMMARY_FIELD, true);
    assertEquals(getData(QUERY_PATH, request, response), HttpStatus.OK_200);
    responseData = response.getAsJsonObject(RESPONSE_FIELD);
    assertTrue(responseData.has(RESPONSE_ITEMS_ARRAY));
    final int numNoFNItems = responseData.get(RESPONSE_ITEMS_ARRAY).getAsJsonArray().size();
    for (final JsonElement elmt : responseData.get(RESPONSE_ITEMS_ARRAY).getAsJsonArray()) {
      final JsonObject item = elmt.getAsJsonObject();
      assertTrue(item.has(RESPONSE_ITEM_VALUE));
      assertTrue(item.has(RESPONSE_ITEM_ESTIMATE));
      assertTrue(item.has(RESPONSE_ITEM_UPPER_BOUND));
      assertTrue(item.has(RESPONSE_ITEM_LOWER_BOUND));
    }

    // should be strictly greater by construction
    assertTrue(numNoFNItems > numNoFPItems);
  }

  @Test
  public void hllQuery() {
    final String sketchName = "hll2";
    final JsonObject response = new JsonObject();
    final JsonObject request = new JsonObject();

    HllSketch sk = (HllSketch) server_.getSketch(sketchName).sketch_;
    sk.update("itemA");
    sk.update("itemB");
    assertFalse(sk.isEmpty());

    request.addProperty(QUERY_NAME_FIELD, sketchName);
    request.addProperty(QUERY_SUMMARY_FIELD, false);
    assertEquals(getData(QUERY_PATH, request, response), HttpStatus.OK_200);

    // get sketch again before testing
    sk = (HllSketch) server_.getSketch(sketchName).sketch_;
    final JsonObject queryData = response.getAsJsonObject(RESPONSE_FIELD);
    assertEquals(queryData.get(QUERY_NAME_FIELD).getAsString(), sketchName);
    assertEquals(queryData.get(RESPONSE_ESTIMATE_FIELD).getAsDouble(), sk.getEstimate());
    assertEquals(queryData.get(RESPONSE_ESTIMATION_MODE_FIELD).getAsBoolean(), sk.isEstimationMode());
    assertEquals(queryData.get(RESPONSE_P1STDEV_FIELD).getAsDouble(), sk.getUpperBound(1));
    assertEquals(queryData.get(RESPONSE_P2STDEV_FIELD).getAsDouble(), sk.getUpperBound(2));
    assertEquals(queryData.get(RESPONSE_P3STDEV_FIELD).getAsDouble(), sk.getUpperBound(3));
    assertEquals(queryData.get(RESPONSE_M1STDEV_FIELD).getAsDouble(), sk.getLowerBound(1));
    assertEquals(queryData.get(RESPONSE_M2STDEV_FIELD).getAsDouble(), sk.getLowerBound(2));
    assertEquals(queryData.get(RESPONSE_M3STDEV_FIELD).getAsDouble(), sk.getLowerBound(3));
    assertFalse(queryData.has(RESPONSE_SUMMARY_FIELD));
  }

  @Test
  public void kllQuery() {
    final String sketchName = "duration";
    final JsonObject response = new JsonObject();
    JsonObject request = new JsonObject();

    // add N(0,1) Gaussian data directly to sketch for non-uniform results
    final KllFloatsSketch sk = (KllFloatsSketch) server_.getSketch(sketchName).sketch_;
    final int nPoints = 10000;
    for (int i = 0; i < nPoints; ++i) {
      sk.update((float) ThreadLocalRandom.current().nextGaussian());
    }

    // quantiles and CDF query
    request.addProperty(QUERY_NAME_FIELD, sketchName);

    final int numCdfPoints = 7;
    final JsonArray cdfRequestValues = new JsonArray(numCdfPoints);
    final float[] cdfRequestData = new float[numCdfPoints];
    int j = 0;
    for (int i = -3; i <= 3; ++i, ++j) {
      cdfRequestValues.add(1.0 * i);
      cdfRequestData[j] = 1.0f * i;
    }
    request.add(QUERY_CDF_VALUES_FIELD_NAME, cdfRequestValues);

    // 4 values, should be able to check results against 0
    final int numFractionsPoints = 4;
    final JsonArray fractionsRequestValues = new JsonArray(numFractionsPoints);
    final double[] fractionsRequestData = new double[numFractionsPoints];
    fractionsRequestValues.add(0.1); fractionsRequestData[0] = 0.1;
    fractionsRequestValues.add(0.3); fractionsRequestData[1] = 0.3;
    fractionsRequestValues.add(0.7); fractionsRequestData[2] = 0.7;
    fractionsRequestValues.add(0.9); fractionsRequestData[3] = 0.9;
    request.add(QUERY_FRACTIONS_NAME_FIELD, fractionsRequestValues);

    assertEquals(postData(QUERY_PATH, request, response), HttpStatus.OK_200);
    JsonObject responseData = response.get(RESPONSE_FIELD).getAsJsonObject();
    assertEquals(responseData.get(QUERY_NAME_FIELD).getAsString(), sketchName);
    assertEquals(responseData.get(RESPONSE_MIN_VALUE).getAsFloat(), sk.getMinValue(), 1e-12);
    assertEquals(responseData.get(RESPONSE_MAX_VALUE).getAsFloat(), sk.getMaxValue(), 1e-12);
    assertTrue(responseData.get(RESPONSE_ESTIMATION_MODE_FIELD).getAsBoolean());
    assertEquals(responseData.get(RESPONSE_STREAM_LENGTH).getAsLong(), sk.getN());

    // ensure we get the expected values back by comparing results from querying
    // the sketch directly
    final double[] ranks = sk.getCDF(cdfRequestData);
    final JsonArray ranksResult = responseData.get(RESPONSE_CDF_LIST).getAsJsonArray();
    assertEquals(ranksResult.size(), ranks.length);
    for (int i = 0; i < numCdfPoints; ++i) {
      final JsonObject entry = ranksResult.get(i).getAsJsonObject();
      assertEquals(entry.get(RESPONSE_RESULT_VALUE).getAsFloat(), cdfRequestData[i], 1e-6);
      assertEquals(entry.get(RESPONSE_RESULT_RANK).getAsFloat(), ranks[i], 1e-6);
    }
    // not trying to test the sketch, so we'll assume the last value is correct

    final float[] quantiles = sk.getQuantiles(fractionsRequestData);
    final JsonArray quantilesResult = responseData.get(RESPONSE_QUANTILE_LIST).getAsJsonArray();
    assertEquals(quantilesResult.size(), quantiles.length);
    for (int i = 0; i < numFractionsPoints; ++i) {
      final JsonObject entry = quantilesResult.get(i).getAsJsonObject();
      assertEquals(entry.get(RESPONSE_RESULT_RANK).getAsFloat(), fractionsRequestData[i], 1e-6);
      assertEquals(entry.get(RESPONSE_RESULT_QUANTILE).getAsFloat(), quantiles[i], 1e-6);
    }


    // PMF query (not in above to ensure inputs and outputs properly align)
    request = new JsonObject();
    request.addProperty(QUERY_NAME_FIELD, sketchName);
    request.addProperty(QUERY_SUMMARY_FIELD, true);

    final int numPmfPoints = 5;
    final JsonArray pmfRequestValues = new JsonArray(numPmfPoints);
    final float[] pmfRequestData = new float[numPmfPoints];
    pmfRequestValues.add(-1.5); pmfRequestData[0] = -1.5f;
    pmfRequestValues.add(-0.5); pmfRequestData[1] = -0.5f;
    pmfRequestValues.add(0.0);  pmfRequestData[2] = 0.0f;
    pmfRequestValues.add(0.5);  pmfRequestData[3] = 0.5f;
    pmfRequestValues.add(1.5);  pmfRequestData[4] = 1.5f;
    request.add(QUERY_PMF_VALUES_FIELD_NAME, pmfRequestValues);

    assertEquals(postData(QUERY_PATH, request, response), HttpStatus.OK_200);
    responseData = response.get(RESPONSE_FIELD).getAsJsonObject();

    final double[] mass = sk.getPMF(pmfRequestData);
    final JsonArray massResult = responseData.get(RESPONSE_PMF_LIST).getAsJsonArray();
    assertEquals(massResult.size(), mass.length);
    for (int i = 0; i < numPmfPoints; ++i) {
      final JsonObject entry = massResult.get(i).getAsJsonObject();
      assertEquals(entry.get(RESPONSE_RESULT_VALUE).getAsFloat(), pmfRequestData[i], 1e-6);
      assertEquals(entry.get(RESPONSE_RESULT_MASS).getAsFloat(), mass[i], 1e-6);
    }

    assertTrue(responseData.has(RESPONSE_SUMMARY_FIELD));
  }

  @Test
  public void klllQueryErrors() {
    final String sketchName = "duration";
    final JsonObject response = new JsonObject();
    final JsonObject request = new JsonObject();

    // don't need data in the sketch since we're presenting invalid query parameters
    request.addProperty(QUERY_NAME_FIELD, sketchName);

    final JsonArray invalidValues = new JsonArray(1);
    invalidValues.add("Not a number");
    request.add(QUERY_FRACTIONS_NAME_FIELD, invalidValues);
    assertEquals(postData(QUERY_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);

    request.remove(QUERY_FRACTIONS_NAME_FIELD);
    request.add(QUERY_PMF_VALUES_FIELD_NAME, invalidValues);
    assertEquals(postData(QUERY_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);

    invalidValues.remove(0);
    invalidValues.add(-1);
    request.remove(QUERY_PMF_VALUES_FIELD_NAME);
    request.add(QUERY_FRACTIONS_NAME_FIELD, invalidValues);
    assertEquals(postData(QUERY_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);
  }

  @Test
  public void thetaQuery() {
    final String sketchName = "theta1";
    final JsonObject response = new JsonObject();
    final JsonObject request = new JsonObject();

    final Union sk = (Union) server_.getSketch(sketchName).sketch_;
    sk.update("item");
    assertFalse(sk.getResult().isEmpty());

    request.addProperty(QUERY_NAME_FIELD, sketchName);
    assertEquals(getData(QUERY_PATH, request, response), HttpStatus.OK_200);

    // get sketch again before testing
    final CompactSketch theta = ((Union) server_.getSketch(sketchName).sketch_).getResult();
    final JsonObject queryData = response.getAsJsonObject(RESPONSE_FIELD);
    assertEquals(queryData.get(QUERY_NAME_FIELD).getAsString(), sketchName);
    assertEquals(queryData.get(RESPONSE_ESTIMATE_FIELD).getAsDouble(), theta.getEstimate());
    assertEquals(queryData.get(RESPONSE_ESTIMATION_MODE_FIELD).getAsBoolean(), theta.isEstimationMode());
    assertEquals(queryData.get(RESPONSE_P1STDEV_FIELD).getAsDouble(), theta.getUpperBound(1));
    assertEquals(queryData.get(RESPONSE_P2STDEV_FIELD).getAsDouble(), theta.getUpperBound(2));
    assertEquals(queryData.get(RESPONSE_P3STDEV_FIELD).getAsDouble(), theta.getUpperBound(3));
    assertEquals(queryData.get(RESPONSE_M1STDEV_FIELD).getAsDouble(), theta.getLowerBound(1));
    assertEquals(queryData.get(RESPONSE_M2STDEV_FIELD).getAsDouble(), theta.getLowerBound(2));
    assertEquals(queryData.get(RESPONSE_M3STDEV_FIELD).getAsDouble(), theta.getLowerBound(3));
    assertFalse(queryData.has(RESPONSE_SUMMARY_FIELD));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void reservoirQuery() {
    final String sketchName = "rs";
    final JsonObject response = new JsonObject();
    final JsonObject request = new JsonObject();

    // add data directly to sketch, ensure it exists
    final int nItems = 300;
    final ReservoirItemsSketch<String> sk = (ReservoirItemsSketch<String>) server_.getSketch(sketchName).sketch_;
    for (int i = 0; i < nItems; ++i) { sk.update("item" + i); }

    request.addProperty(QUERY_NAME_FIELD, sketchName);
    request.addProperty(QUERY_SUMMARY_FIELD, true);
    assertEquals(getData(QUERY_PATH, request, response), HttpStatus.OK_200);

    final JsonObject responseData = response.get(RESPONSE_FIELD).getAsJsonObject();
    assertEquals(responseData.get(QUERY_NAME_FIELD).getAsString(), sketchName);
    assertFalse(responseData.get(RESPONSE_SUMMARY_FIELD).getAsString().isEmpty());
    assertEquals(responseData.get(RESPONSE_SKETCH_K).getAsInt(), sk.getK());
    assertEquals(responseData.get(RESPONSE_STREAM_LENGTH).getAsLong(), sk.getN());
    assertEquals(responseData.get(RESPONSE_SKETCH_K).getAsInt(), sk.getK());
    assertEquals(responseData.get(RESPONSE_ITEMS_ARRAY).getAsJsonArray().size(), Math.min(sk.getN(), sk.getK()));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void varOptQuery() {
    final String sketchName = "vo";
    final JsonObject response = new JsonObject();
    final JsonObject request = new JsonObject();

    // add data directly to sketch, ensure it exists
    final int nItems = 50;
    final VarOptItemsSketch<String> sk = (VarOptItemsSketch<String>) server_.getSketch(sketchName).sketch_;
    double cumulativeWeight = 0.0;
    for (int i = 1; i <= nItems; ++i) { // starting at 1 to avoid a 0 weight
      final double weight = Math.pow(1.0 * i, 3);
      cumulativeWeight += weight;
      sk.update("item", weight);
    }

    // reset, then check sketch is again empty
    request.addProperty(QUERY_NAME_FIELD, sketchName);
    assertEquals(getData(QUERY_PATH, request, response), HttpStatus.OK_200);

    final JsonObject responseData = response.get(RESPONSE_FIELD).getAsJsonObject();
    assertEquals(responseData.get(QUERY_NAME_FIELD).getAsString(), sketchName);
    assertEquals(responseData.get(RESPONSE_SKETCH_K).getAsInt(), sk.getK());
    assertEquals(responseData.get(RESPONSE_STREAM_LENGTH).getAsLong(), sk.getN());
    assertEquals(responseData.get(RESPONSE_SKETCH_K).getAsInt(), sk.getK());

    final JsonArray itemData = responseData.get(RESPONSE_ITEMS_ARRAY).getAsJsonArray();
    assertEquals(itemData.size(), Math.min(sk.getN(), sk.getK()));
    double totalResponseWeight = 0.0;
    for (final JsonElement elmt : itemData) {
      final JsonObject obj = elmt.getAsJsonObject();
      assertTrue(obj.has(RESPONSE_ITEM_VALUE));
      assertTrue(obj.has(RESPONSE_ITEM_WEIGHT));
      totalResponseWeight += obj.get(RESPONSE_ITEM_WEIGHT).getAsDouble();
    }
    assertEquals(totalResponseWeight, cumulativeWeight, 1e-15);
  }
}
