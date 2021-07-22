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

import javax.servlet.http.HttpServletResponse;

import org.apache.datasketches.cpc.CpcSketch;
import org.testng.annotations.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;


public class UpdateHandlerTest extends ServerTestBase {
  /* The tests here are going to be structured very similarly. It might be possible
   * to find a common framework and reduce the amount o repetition? But not clear with
   * type erasure as opposed to C++-style templates.
   */

  @Test
  public void cpcUpdate() {
    final JsonObject response = new JsonObject();
    final String sketchName = "cpcOfNumbers";
    final int nPoints = 1000;

    // testing using both GET and POST
    int status;

    JsonObject request = new JsonObject();
    JsonArray data = new JsonArray();
    for (int i = 0; i < nPoints; ++i)
      data.add(i);
    request.add(sketchName, data);
    status = postData(UPDATE_PATH, request, response);
    assertEquals(status, HttpServletResponse.SC_OK);

    request = new JsonObject();
    data = new JsonArray();
    for (int i = nPoints; i < 2 * nPoints; ++i)
      data.add(i);
    request.add(sketchName, data);
    status = getData(UPDATE_PATH, request, response);
    assertEquals(status, HttpServletResponse.SC_OK);

    final JsonElement element = response.get(RESPONSE_FIELD);
    assertTrue(element.isJsonNull());

    final SketchStorage.SketchEntry entry = server_.getSketch(sketchName);
    final CpcSketch sk = (CpcSketch) entry.sketch;
    assertEquals(sk.getEstimate(), 2 * nPoints, 2 * nPoints * 1e-2);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void fiUpdate() {
    final JsonObject response = new JsonObject();
    final String sketchName = "topItems";

    // single item
    JsonObject request = new JsonObject();
    request.addProperty(sketchName, "item1");
    assertEquals(postData(UPDATE_PATH, request, response), HttpServletResponse.SC_OK);

    // item with weight
    request = new JsonObject();
    JsonObject data = new JsonObject();
    data.addProperty(QUERY_PAIR_ITEM_FIELD, "item2");
    data.addProperty(QUERY_PAIR_WEIGHT_FIELD, 5);
    request.add(sketchName, data);
    assertEquals(postData(UPDATE_PATH, request, response), HttpServletResponse.SC_OK);

    // array of items with and without weights
    request = new JsonObject();
    final JsonArray dataArray = new JsonArray();
    dataArray.add("item1"); // increases count to 2
    data = new JsonObject();
    data.addProperty(QUERY_PAIR_ITEM_FIELD, "item3");
    data.addProperty(QUERY_PAIR_WEIGHT_FIELD, 10);
    dataArray.add(data);
    request.add(sketchName, dataArray);
    assertEquals(postData(UPDATE_PATH, request, response), HttpServletResponse.SC_OK);

    final JsonElement element = response.get(RESPONSE_FIELD);
    assertTrue(element.isJsonNull());

    final SketchStorage.SketchEntry entry = server_.getSketch(sketchName);
    final org.apache.datasketches.frequencies.ItemsSketch<String> sk = (org.apache.datasketches.frequencies.ItemsSketch<String>) entry.sketch;
    assertEquals(sk.getEstimate("item1"), 2);
    assertEquals(sk.getEstimate("item2"), 5);
    assertEquals(sk.getEstimate("item3"), 10);
  }

  @Test
  public void hllUpdate() {
    // update multiple sketches from an array
  }

  @Test
  public void kllUpdate() {

  }

  @Test
  public void thetaUpdate() {

  }

  @Test
  public void reservoirUpdate() {

  }

  @Test
  public void voUpdate() {

  }

}
