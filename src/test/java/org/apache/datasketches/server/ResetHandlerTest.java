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

import static org.apache.datasketches.server.SketchConstants.QUERY_NAME_FIELD;
import static org.apache.datasketches.server.SketchConstants.RESET_PATH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.sampling.ReservoirItemsSketch;
import org.apache.datasketches.sampling.VarOptItemsSketch;
import org.apache.datasketches.theta.Union;
import org.eclipse.jetty.http.HttpStatus;
import org.testng.annotations.Test;

import com.google.gson.JsonObject;

public class ResetHandlerTest extends ServerTestBase {
  @Test
  public void emptyReset() {
    final JsonObject response = new JsonObject();
    JsonObject request = new JsonObject();

    // completely empty request cannot be handled
    assertEquals(postData(RESET_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);

    // invalid name field with real sketch name
    request.addProperty("notAName", "theta0");
    assertEquals(postData(RESET_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);

    // valid name, empty value
    request = new JsonObject();
    request.add(QUERY_NAME_FIELD, new JsonObject());
    assertEquals(postData(RESET_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);

    // valid name, invalid sketch name
    request = new JsonObject();
    request.addProperty(QUERY_NAME_FIELD, "sketchDoesNotExist");
    assertEquals(postData(RESET_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);
  }

  @Test
  public void cpcReset() {
    final String sketchName = "cpcOfLongs";
    final JsonObject response = new JsonObject();
    final JsonObject request = new JsonObject();

    // add data directly to sketch, ensure it exists
    CpcSketch sk = (CpcSketch) server_.getSketch(sketchName).sketch_;
    for (int i = 0; i < 5; ++i) { sk.update(i); }
    assertFalse(sk.isEmpty());

    // reset, then check sketch is again empty
    request.addProperty(QUERY_NAME_FIELD, sketchName);
    assertEquals(getData(RESET_PATH, request, response), HttpStatus.OK_200);
    assertTrue(response.get(RESPONSE_FIELD).isJsonNull());

    // get sketch again before testing
    sk = (CpcSketch) server_.getSketch(sketchName).sketch_;
    assertTrue(sk.isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void fiReset() {
    final String sketchName = "topItems";
    final JsonObject response = new JsonObject();
    final JsonObject request = new JsonObject();

    // add data directly to sketch, ensure it exists
    ItemsSketch<String> sk = (ItemsSketch<String>) server_.getSketch(sketchName).sketch_;
    sk.update("itemA", 3);
    sk.update("itemB", 1);
    assertFalse(sk.isEmpty());

    // reset, then check sketch is again empty
    request.addProperty(QUERY_NAME_FIELD, sketchName);
    assertEquals(getData(RESET_PATH, request, response), HttpStatus.OK_200);
    assertTrue(response.get(RESPONSE_FIELD).isJsonNull());

    // get sketch again before testing
    sk = (ItemsSketch<String>) server_.getSketch(sketchName).sketch_;
    assertTrue(sk.isEmpty());
  }

  @Test
  public void hllReset() {
    final String sketchName = "hll2";
    final JsonObject response = new JsonObject();
    final JsonObject request = new JsonObject();

    // add data directly to sketch, ensure it exists
    HllSketch sk = (HllSketch) server_.getSketch(sketchName).sketch_;
    sk.update("itemA");
    sk.update("itemB");
    assertFalse(sk.isEmpty());

    // reset, then check sketch is again empty
    request.addProperty(QUERY_NAME_FIELD, sketchName);
    assertEquals(getData(RESET_PATH, request, response), HttpStatus.OK_200);
    assertTrue(response.get(RESPONSE_FIELD).isJsonNull());

    // get sketch again before testing
    sk = (HllSketch) server_.getSketch(sketchName).sketch_;
    assertTrue(sk.isEmpty());
  }

  @Test
  public void kllReset() {
    final String sketchName = "duration";
    final JsonObject response = new JsonObject();
    final JsonObject request = new JsonObject();

    // add data directly to sketch, ensure it exists
    KllFloatsSketch sk = (KllFloatsSketch) server_.getSketch(sketchName).sketch_;
    sk.update(128.0f);
    assertFalse(sk.isEmpty());

    // reset, then check sketch is again empty
    request.addProperty(QUERY_NAME_FIELD, sketchName);
    assertEquals(getData(RESET_PATH, request, response), HttpStatus.OK_200);
    assertTrue(response.get(RESPONSE_FIELD).isJsonNull());

    // get sketch again before testing
    sk = (KllFloatsSketch) server_.getSketch(sketchName).sketch_;
    assertTrue(sk.isEmpty());
  }

  @Test
  public void thetaReset() {
    final String sketchName = "theta1";
    final JsonObject response = new JsonObject();
    final JsonObject request = new JsonObject();

    // add data directly to sketch, ensure it exists
    Union sk = (Union) server_.getSketch(sketchName).sketch_;
    sk.update("item");
    assertFalse(sk.getResult().isEmpty());

    // reset, then check sketch is again empty
    request.addProperty(QUERY_NAME_FIELD, sketchName);
    assertEquals(getData(RESET_PATH, request, response), HttpStatus.OK_200);
    assertTrue(response.get(RESPONSE_FIELD).isJsonNull());

    // get sketch again before testing
    sk = (Union) server_.getSketch(sketchName).sketch_;
    assertTrue(sk.getResult().isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void reservoirReset() {
    final String sketchName = "rs";
    final JsonObject response = new JsonObject();
    final JsonObject request = new JsonObject();

    // add data directly to sketch, ensure it exists
    ReservoirItemsSketch<String> sk = (ReservoirItemsSketch<String>) server_.getSketch(sketchName).sketch_;
    sk.update("item");
    assertTrue(sk.getNumSamples() > 0);

    // reset, then check sketch is again empty
    request.addProperty(QUERY_NAME_FIELD, sketchName);
    assertEquals(getData(RESET_PATH, request, response), HttpStatus.OK_200);
    assertTrue(response.get(RESPONSE_FIELD).isJsonNull());

    // get sketch again before testing
    sk = (ReservoirItemsSketch<String>) server_.getSketch(sketchName).sketch_;
    assertEquals(sk.getNumSamples(), 0);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void varOptReset() {
    final String sketchName = "vo";
    final JsonObject response = new JsonObject();
    final JsonObject request = new JsonObject();

    // add data directly to sketch, ensure it exists
    VarOptItemsSketch<String> sk = (VarOptItemsSketch<String>) server_.getSketch(sketchName).sketch_;
    sk.update("item", 5);
    assertTrue(sk.getNumSamples() > 0);

    // reset, then check sketch is again empty
    request.addProperty(QUERY_NAME_FIELD, sketchName);
    assertEquals(getData(RESET_PATH, request, response), HttpStatus.OK_200);
    assertTrue(response.get(RESPONSE_FIELD).isJsonNull());

    // get sketch again before testing
    sk = (VarOptItemsSketch<String>) server_.getSketch(sketchName).sketch_;
    assertEquals(sk.getNumSamples(), 0);
  }
}
