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

import static org.apache.datasketches.server.SketchConstants.QUERY_PAIR_ITEM_FIELD;
import static org.apache.datasketches.server.SketchConstants.QUERY_PAIR_WEIGHT_FIELD;
import static org.apache.datasketches.server.SketchConstants.UPDATE_PATH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.datasketches.cpc.CpcSketch;
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
import com.google.gson.JsonPrimitive;

public class UpdateHandlerTest extends ServerTestBase {
  /* The tests here are going to be structured very similarly. It might be possible
   * to find a common framework and reduce the amount ofXS repetition?
   */
  @Test
  public void emptyUpdate() {
    final JsonObject response = new JsonObject();
    JsonObject request = new JsonObject();

    // completely empty request cannot be handled
    assertEquals(postData(UPDATE_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);

    // invalid name with value
    request.addProperty("sketchDoesNotExist", "validStringValue");
    assertEquals(postData(UPDATE_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);

    // valid name, empty value
    request = new JsonObject();
    request.add("hll1", new JsonObject());
    assertEquals(postData(UPDATE_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);
  }

  @Test
  public void cpcUpdate1() {
    final JsonObject response = new JsonObject();
    final String sketchName = "cpcOfLongs";
    final int nPoints = 1000;

    // testing using both GET and POST
    JsonObject request = new JsonObject();
    JsonArray data = new JsonArray();
    for (int i = 0; i < nPoints; ++i)
      data.add(i);
    request.add(sketchName, data);
    assertEquals(postData(UPDATE_PATH, request, response), HttpStatus.OK_200);
    assertEquals(response.size(), 0);

    request = new JsonObject();
    data = new JsonArray();
    for (int i = nPoints; i < 2 * nPoints; ++i)
      data.add(i);
    request.add(sketchName, data);
    assertEquals(getData(UPDATE_PATH, request, response), HttpStatus.OK_200);
    assertEquals(response.size(), 0);

    // single-item update
    request = new JsonObject();
    request.add(sketchName, new JsonPrimitive(-1));
    assertEquals(getData(UPDATE_PATH, request, response), HttpStatus.OK_200);
    assertEquals(response.size(), 0);

    final SketchStorage.SketchEntry entry = server_.getSketch(sketchName);
    final CpcSketch sk = (CpcSketch) entry.sketch_;
    assertEquals(entry.type_, ValueType.LONG);
    assertTrue(2 * nPoints + 1 <= sk.getUpperBound(1));
    assertTrue(2 * nPoints + 1 >= sk.getLowerBound(1));
  }

  @Test
  public void cpcUpdate2() {
    final JsonObject response = new JsonObject();
    final String sketchName = "cpcOfStrings";
    final int nPoints = 5000;

    JsonObject request = new JsonObject();
    final JsonArray data = new JsonArray();
    for (int i = 0; i < nPoints; ++i)
      data.add(i); // not converting to String since should happen upon parsing
    request.add(sketchName, data);
    assertEquals(postData(UPDATE_PATH, request, response), HttpStatus.OK_200);
    assertEquals(response.size(), 0);

    request = new JsonObject();
    request.add(sketchName, new JsonPrimitive(-1));
    assertEquals(postData(UPDATE_PATH, request, response), HttpStatus.OK_200);
    assertEquals(response.size(), 0);

    final SketchStorage.SketchEntry entry = server_.getSketch(sketchName);
    final CpcSketch sk = (CpcSketch) entry.sketch_;
    assertEquals(entry.type_, ValueType.STRING);

    assertTrue(nPoints + 1 <= sk.getUpperBound(1));
    assertTrue(nPoints + 1 >= sk.getLowerBound(1));
  }

  @Test
  public void cpcUpdate3() {
    final JsonObject response = new JsonObject();
    final String sketchName = "cpcOfFloats";
    final int nPoints = 2500;

    JsonObject request = new JsonObject();
    final JsonArray data = new JsonArray();
    for (int i = 0; i < nPoints; ++i)
      data.add(i * 10.0);
    request.add(sketchName, data);
    assertEquals(postData(UPDATE_PATH, request, response), HttpStatus.OK_200);
    assertEquals(response.size(), 0);

    request = new JsonObject();
    request.add(sketchName, new JsonPrimitive(-1));
    assertEquals(getData(UPDATE_PATH, request, response), HttpStatus.OK_200);
    assertEquals(response.size(), 0);

    final SketchStorage.SketchEntry entry = server_.getSketch(sketchName);
    final CpcSketch sk = (CpcSketch) entry.sketch_;
    assertEquals(entry.type_, ValueType.FLOAT);
    assertTrue(nPoints + 1 <= sk.getUpperBound(1));
    assertTrue(nPoints + 1 >= sk.getLowerBound(1));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void fiUpdate() {
    final JsonObject response = new JsonObject();
    final String sketchName = "topItems";

    // single item
    JsonObject request = new JsonObject();
    request.addProperty(sketchName, "item1");
    assertEquals(postData(UPDATE_PATH, request, response), HttpStatus.OK_200);
    assertEquals(response.size(), 0);

    // item with weight
    request = new JsonObject();
    JsonObject data = new JsonObject();
    data.addProperty(QUERY_PAIR_ITEM_FIELD, "item2");
    data.addProperty(QUERY_PAIR_WEIGHT_FIELD, 5);
    request.add(sketchName, data);
    assertEquals(postData(UPDATE_PATH, request, response), HttpStatus.OK_200);
    assertEquals(response.size(), 0);

    // array of items with and without weights
    request = new JsonObject();
    final JsonArray dataArray = new JsonArray();
    dataArray.add("item1"); // increases count to 2
    data = new JsonObject();
    data.addProperty(QUERY_PAIR_ITEM_FIELD, "item3");
    data.addProperty(QUERY_PAIR_WEIGHT_FIELD, 10);
    dataArray.add(data);
    request.add(sketchName, dataArray);
    assertEquals(postData(UPDATE_PATH, request, response), HttpStatus.OK_200);
    assertEquals(response.size(), 0);

    final SketchStorage.SketchEntry entry = server_.getSketch(sketchName);
    final org.apache.datasketches.frequencies.ItemsSketch<String> sk = (org.apache.datasketches.frequencies.ItemsSketch<String>) entry.sketch_;
    assertEquals(sk.getEstimate("item1"), 2);
    assertEquals(sk.getEstimate("item2"), 5);
    assertEquals(sk.getEstimate("item3"), 10);

    // update with malformed item/weight pair (each independently)
    request = new JsonObject();
    data = new JsonObject();
    data.addProperty("invalid", "item2");
    data.addProperty(QUERY_PAIR_WEIGHT_FIELD, 5);
    request.add(sketchName, data);
    assertEquals(postData(UPDATE_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);

    data = new JsonObject();
    data.addProperty(QUERY_PAIR_ITEM_FIELD, "item2");
    data.addProperty("invalid", 5);
    request.add(sketchName, data);
    assertEquals(postData(UPDATE_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);
  }

  @Test
  public void hllUpdate() {
    final JsonObject response = new JsonObject();
    final String sketchName = "hll4";
    final int nPoints = 100;

    JsonObject request = new JsonObject();
    final JsonArray data = new JsonArray();
    for (int i = 0; i < nPoints; ++i)
      data.add(i); // not converting to String since should happen upon parsing
    request.add(sketchName, data);
    assertEquals(postData(UPDATE_PATH, request, response), HttpStatus.OK_200);
    assertEquals(response.size(), 0);

    request = new JsonObject();
    request.add(sketchName, new JsonPrimitive(-1));
    assertEquals(postData(UPDATE_PATH, request, response), HttpStatus.OK_200);
    assertEquals(response.size(), 0);

    final SketchStorage.SketchEntry entry = server_.getSketch(sketchName);
    final HllSketch sk = (HllSketch) entry.sketch_;
    assertEquals(entry.type_, ValueType.STRING);
    assertTrue(nPoints + 1 <= sk.getUpperBound(1));
    assertTrue(nPoints + 1 >= sk.getLowerBound(1));
  }

  @Test
  public void hllUpdate2() {
    final JsonObject response = new JsonObject();
    final String sketchName = "hllOfInts";
    final int nPoints = 20; // should be in exact mode

    JsonObject request = new JsonObject();
    JsonArray data = new JsonArray();
    for (int i = 0; i < nPoints; ++i)
      data.add(i);
    request.add(sketchName, data);
    assertEquals(getData(UPDATE_PATH, request, response), HttpStatus.OK_200);
    assertEquals(response.size(), 0);

    request = new JsonObject();
    data = new JsonArray();
    for (int i = nPoints; i < 2 * nPoints; ++i)
      data.add(i);
    request.add(sketchName, data);
    assertEquals(getData(UPDATE_PATH, request, response), HttpStatus.OK_200);
    assertEquals(response.size(), 0);

    // single-item update
    request = new JsonObject();
    request.add(sketchName, new JsonPrimitive(-1));
    assertEquals(getData(UPDATE_PATH, request, response), HttpStatus.OK_200);
    assertEquals(response.size(), 0);

    final SketchStorage.SketchEntry entry = server_.getSketch(sketchName);
    final HllSketch sk = (HllSketch) entry.sketch_;
    assertEquals(entry.type_, ValueType.INT);
    assertTrue(2 * nPoints + 1 <= sk.getUpperBound(1));
    assertTrue(2 * nPoints + 1 >= sk.getLowerBound(1));
  }

  @Test
  public void hllUpdate3() {
    final JsonObject response = new JsonObject();
    final String sketchName = "hllOfDoubles";
    final int nPoints = 479;

    JsonObject request = new JsonObject();
    final JsonArray data = new JsonArray();
    for (int i = 0; i < nPoints; ++i)
      data.add(i * 10.0);
    request.add(sketchName, data);
    assertEquals(postData(UPDATE_PATH, request, response), HttpStatus.OK_200);
    assertEquals(response.size(), 0);

    request = new JsonObject();
    request.add(sketchName, new JsonPrimitive(-1));
    assertEquals(getData(UPDATE_PATH, request, response), HttpStatus.OK_200);
    assertEquals(response.size(), 0);

    final SketchStorage.SketchEntry entry = server_.getSketch(sketchName);
    final HllSketch sk = (HllSketch) entry.sketch_;
    assertEquals(entry.type_, ValueType.DOUBLE);
    assertTrue(nPoints + 1 <= sk.getUpperBound(1));
    assertTrue(nPoints + 1 >= sk.getLowerBound(1));
  }

  @Test
  public void kllUpdate() {
    final JsonObject response = new JsonObject();
    final String sketchName = "duration";
    final int nPoints = 953;
    final int nUpdates = 5;

    for (int j = 0; j < nUpdates; ++j) {
      // batch update
      JsonObject request = new JsonObject();
      final JsonArray data = new JsonArray();
      for (int i = 0; i < nPoints; ++i)
        data.add(i * 10.0);
      request.add(sketchName, data);
      assertEquals(postData(UPDATE_PATH, request, response), HttpStatus.OK_200);
      assertEquals(response.size(), 0);

      // single update
      request = new JsonObject();
      request.add(sketchName, new JsonPrimitive(-1));
      assertEquals(getData(UPDATE_PATH, request, response), HttpStatus.OK_200);
      assertEquals(response.size(), 0);
    }

    final SketchStorage.SketchEntry entry = server_.getSketch(sketchName);
    final KllFloatsSketch sk = (KllFloatsSketch) entry.sketch_;
    assertEquals(sk.getN(), (nPoints + 1) * nUpdates);
  }

  @Test
  public void thetaUpdate1() {
    final JsonObject response = new JsonObject();
    final String sketchName = "theta0";
    final int nPoints = 1000;

    JsonObject request = new JsonObject();
    JsonArray data = new JsonArray();
    for (int i = 0; i < nPoints; ++i)
      data.add(i);
    request.add(sketchName, data);
    assertEquals(postData(UPDATE_PATH, request, response), HttpStatus.OK_200);
    assertEquals(response.size(), 0);

    request = new JsonObject();
    data = new JsonArray();
    for (int i = nPoints; i < 2 * nPoints; ++i)
      data.add(i);
    request.add(sketchName, data);
    assertEquals(getData(UPDATE_PATH, request, response), HttpStatus.OK_200);
    assertEquals(response.size(), 0);

    // single-item update
    request = new JsonObject();
    request.add(sketchName, new JsonPrimitive(-1));
    assertEquals(getData(UPDATE_PATH, request, response), HttpStatus.OK_200);
    assertEquals(response.size(), 0);

    final SketchStorage.SketchEntry entry = server_.getSketch(sketchName);
    final CompactSketch sk = ((Union) entry.sketch_).getResult();
    assertEquals(entry.type_, ValueType.INT);
    assertTrue(2 * nPoints + 1 <= sk.getUpperBound(1));
    assertTrue(2 * nPoints + 1 >= sk.getLowerBound(1));
  }

  @Test
  public void thetaUpdate2() {
    final JsonObject response = new JsonObject();
    final String sketchName = "thetaOfStrings";
    final int nPoints = 5000;

    JsonObject request = new JsonObject();
    final JsonArray data = new JsonArray();
    for (int i = 0; i < nPoints; ++i)
      data.add(i); // not converting to String since should happen upon parsing
    request.add(sketchName, data);
    assertEquals(postData(UPDATE_PATH, request, response), HttpStatus.OK_200);
    assertEquals(response.size(), 0);

    request = new JsonObject();
    request.add(sketchName, new JsonPrimitive(-1));
    assertEquals(postData(UPDATE_PATH, request, response), HttpStatus.OK_200);
    assertEquals(response.size(), 0);

    final SketchStorage.SketchEntry entry = server_.getSketch(sketchName);
    final CompactSketch sk = ((Union) entry.sketch_).getResult();
    assertEquals(entry.type_, ValueType.STRING);
    assertTrue(nPoints + 1 <= sk.getUpperBound(1));
    assertTrue(nPoints + 1 >= sk.getLowerBound(1));
  }

  @Test
  public void thetaUpdate3() {
    final JsonObject response = new JsonObject();
    final String sketchName = "thetaOfDoubles";
    final int nPoints = 1153;

    JsonObject request = new JsonObject();
    final JsonArray data = new JsonArray();
    for (int i = 0; i < nPoints; ++i)
      data.add(i * 10.0);
    request.add(sketchName, data);
    assertEquals(postData(UPDATE_PATH, request, response), HttpStatus.OK_200);
    assertEquals(response.size(), 0);

    request = new JsonObject();
    request.add(sketchName, new JsonPrimitive(-1));
    assertEquals(getData(UPDATE_PATH, request, response), HttpStatus.OK_200);
    assertEquals(response.size(), 0);

    final SketchStorage.SketchEntry entry = server_.getSketch(sketchName);
    final CompactSketch sk = ((Union) entry.sketch_).getResult();
    assertEquals(entry.type_, ValueType.DOUBLE);
    assertTrue(nPoints + 1 <= sk.getUpperBound(1));
    assertTrue(nPoints + 1 >= sk.getLowerBound(1));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void reservoirUpdate() {
    final JsonObject response = new JsonObject();
    final String sketchName = "rs";
    final int nPoints = 20;
    final int nUpdates = 5;

    // just going to use numbers as Strings to avoid creating a dictionary
    for (int j = 0; j < nUpdates; ++j) {
      JsonObject request = new JsonObject();
      final JsonArray data = new JsonArray();
      for (int i = 0; i < nPoints; ++i)
        data.add(Integer.toString(i));
      request.add(sketchName, data);
      assertEquals(postData(UPDATE_PATH, request, response), HttpStatus.OK_200);
      assertEquals(response.size(), 0);

      // single update
      request = new JsonObject();
      request.add(sketchName, new JsonPrimitive("-1"));
      assertEquals(getData(UPDATE_PATH, request, response), HttpStatus.OK_200);
      assertEquals(response.size(), 0);
    }

    final SketchStorage.SketchEntry entry = server_.getSketch(sketchName);
    final ReservoirItemsSketch<String> sk = (ReservoirItemsSketch<String>) entry.sketch_;
    assertEquals(sk.getN(), (nPoints + 1) * nUpdates);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void varOptUpdate() {
    final JsonObject response = new JsonObject();
    final String sketchName = "vo";

    // single item
    JsonObject request = new JsonObject();
    request.addProperty(sketchName, "item1");
    assertEquals(postData(UPDATE_PATH, request, response), HttpStatus.OK_200);
    assertEquals(response.size(), 0);

    // item with weight
    request = new JsonObject();
    JsonObject data = new JsonObject();
    data.addProperty(QUERY_PAIR_ITEM_FIELD, "item2");
    data.addProperty(QUERY_PAIR_WEIGHT_FIELD, 5);
    request.add(sketchName, data);
    assertEquals(postData(UPDATE_PATH, request, response), HttpStatus.OK_200);
    assertEquals(response.size(), 0);

    // array of items with and without weights
    request = new JsonObject();
    final JsonArray dataArray = new JsonArray();
    dataArray.add("item1"); // that it's a duplicate doens't matter
    data = new JsonObject();
    data.addProperty(QUERY_PAIR_ITEM_FIELD, "item3");
    data.addProperty(QUERY_PAIR_WEIGHT_FIELD, 10);
    dataArray.add(data);
    request.add(sketchName, dataArray);
    assertEquals(postData(UPDATE_PATH, request, response), HttpStatus.OK_200);
    assertEquals(response.size(), 0);

    final SketchStorage.SketchEntry entry = server_.getSketch(sketchName);
    final VarOptItemsSketch<String> sk = (VarOptItemsSketch<String>) entry.sketch_;
    assertEquals(sk.getN(), 4);
  }
}
