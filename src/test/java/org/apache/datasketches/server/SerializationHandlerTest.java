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
import static org.apache.datasketches.server.SketchConstants.QUERY_SKETCH_FIELD;
import static org.apache.datasketches.server.SketchConstants.SERIALIZE_PATH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.Base64;

import org.apache.datasketches.ArrayOfStringsSerDe;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.sampling.ReservoirItemsSketch;
import org.apache.datasketches.sampling.VarOptItemsSketch;
import org.apache.datasketches.theta.Union;
import org.eclipse.jetty.http.HttpStatus;
import org.testng.annotations.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class SerializationHandlerTest extends ServerTestBase {
  void checkSerializedResult(final byte[] skBytes, final String tgtSketchName) {
    final JsonObject response = new JsonObject();
    final JsonObject request = new JsonObject();

    request.addProperty(QUERY_NAME_FIELD, tgtSketchName);
    assertEquals(getData(SERIALIZE_PATH, request, response), HttpStatus.OK_200);
    final JsonObject result = response.get(RESPONSE_FIELD).getAsJsonObject();

    // expecting a single entry in the response, so decode and compare against serialized sketch
    assertEquals(result.get(QUERY_NAME_FIELD).getAsString(), tgtSketchName);
    final String b64Data = result.get(QUERY_SKETCH_FIELD).getAsString();
    assertEquals(Base64.getUrlDecoder().decode(b64Data), skBytes);
  }

  @Test
  public void emptySerialize() {
    final JsonObject response = new JsonObject();
    JsonObject request = new JsonObject();

    // completely empty request cannot be handled
    assertEquals(postData(SERIALIZE_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);

    // invalid name field with real sketch name
    request.addProperty("notAName", "theta0");
    assertEquals(postData(SERIALIZE_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);

    // valid name field, empty value
    request = new JsonObject();
    request.add(QUERY_NAME_FIELD, new JsonObject());
    assertEquals(postData(SERIALIZE_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);

    // valid name, invalid sketch name
    request = new JsonObject();
    request.addProperty(QUERY_NAME_FIELD, "sketchDoesNotExist");
    assertEquals(postData(SERIALIZE_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);
  }

  @Test
  public void cpcSerialize() {
    final String sketchName = "cpcOfLongs";

    // add data directly to sketch
    final CpcSketch sk = (CpcSketch) server_.getSketch(sketchName).sketch_;
    for (int i = 0; i < 5; ++i) { sk.update(i); }
    assertFalse(sk.isEmpty());
    final byte[] skBytes = sk.toByteArray();

    checkSerializedResult(skBytes, sketchName);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void fiSerialize() {
    final String sketchName = "topItems";

    final ItemsSketch<String> sk = (ItemsSketch<String>) server_.getSketch(sketchName).sketch_;
    sk.update("itemA", 3);
    sk.update("itemB", 1);
    final byte[] skBytes = sk.toByteArray(new ArrayOfStringsSerDe());

    checkSerializedResult(skBytes, sketchName);
  }

  @Test
  public void hllSerialization() {
    final String sketchName1 = "hll1";
    final String sketchName2 = "hll2";

    // one one data, one empty, sent in as an array
    final HllSketch sk1 = (HllSketch) server_.getSketch(sketchName1).sketch_;
    sk1.update("itemA");
    sk1.update("itemB");
    final byte[] sk1Bytes = sk1.toCompactByteArray();
    final HllSketch sk2 = (HllSketch) server_.getSketch(sketchName2).sketch_;
    final byte[] sk2Bytes = sk2.toCompactByteArray();

    // create input as an array
    final JsonArray request = new JsonArray();
    final JsonObject obj1 = new JsonObject();
    obj1.addProperty(QUERY_NAME_FIELD, sketchName1);
    request.add(obj1);
    final JsonObject obj2 = new JsonObject();
    obj2.addProperty(QUERY_NAME_FIELD, sketchName2);
    request.add(obj2);

    final JsonObject response = new JsonObject();
    assertEquals(getData(SERIALIZE_PATH, request, response), HttpStatus.OK_200);
    final JsonArray responseData = response.getAsJsonArray(RESPONSE_FIELD);
    assertEquals(responseData.size(), 2);

    boolean foundSk1 = false;
    boolean foundSk2 = false;
    String b64Data;
    for (final JsonElement e : responseData) {
      final JsonObject o = e.getAsJsonObject();
      switch (o.get(QUERY_NAME_FIELD).getAsString()) {
        case sketchName1:
          b64Data = o.get(QUERY_SKETCH_FIELD).getAsString();
          assertEquals(Base64.getUrlDecoder().decode(b64Data), sk1Bytes);
          foundSk1 = true;
          break;
        case sketchName2:
          b64Data = o.get(QUERY_SKETCH_FIELD).getAsString();
          assertEquals(Base64.getUrlDecoder().decode(b64Data), sk2Bytes);
          foundSk2 = true;
          break;
        default:
          fail();
      }
    }
    assertTrue(foundSk1 && foundSk2);
  }

  @Test
  public void kllSerialization() {
    final String sketchName = "duration";

    final KllFloatsSketch sk = (KllFloatsSketch) server_.getSketch(sketchName).sketch_;
    for (int i = 0; i < 2942; ++i) { sk.update(i * 128.0f); }
    final byte[] skBytes = sk.toByteArray();

    checkSerializedResult(skBytes, sketchName);
  }

  @Test
  public void thetaSerialization() {
    final String sketchName = "theta1";

    final Union sk = (Union) server_.getSketch(sketchName).sketch_;
    sk.update("item");
    final byte[] skBytes = sk.getResult().toByteArray();

    checkSerializedResult(skBytes, sketchName);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void reservoirReset() {
    final String sketchName = "rs";

    final ReservoirItemsSketch<String> sk = (ReservoirItemsSketch<String>) server_.getSketch(sketchName).sketch_;
    for (int i = 0; i < 500; ++i) { sk.update("item" + i); }
    final byte[] skBytes = sk.toByteArray(new ArrayOfStringsSerDe());

    checkSerializedResult(skBytes, sketchName);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void varOptReset() {
    final String sketchName = "vo";

    // add some light items and a few really heavy things
    VarOptItemsSketch<String> sk = (VarOptItemsSketch<String>) server_.getSketch(sketchName).sketch_;
    for (int i = 0; i < 250; ++i) { sk.update("item" + i, 1.0); }
    sk.update("heavyItem1", 1e6);
    sk.update("heavyItem2", 1.1e6);
    final byte[] skBytes = sk.toByteArray(new ArrayOfStringsSerDe());

    checkSerializedResult(skBytes, sketchName);
  }
}
