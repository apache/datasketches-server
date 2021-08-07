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

import static org.apache.datasketches.server.SketchConstants.MERGE_PATH;
import static org.apache.datasketches.server.SketchConstants.QUERY_DATA_FIELD;
import static org.apache.datasketches.server.SketchConstants.QUERY_FAMILY_FIELD;
import static org.apache.datasketches.server.SketchConstants.QUERY_MERGE_K_FIELD;
import static org.apache.datasketches.server.SketchConstants.QUERY_MERGE_SRC_FIELD;
import static org.apache.datasketches.server.SketchConstants.QUERY_MERGE_TGT_FIELD;
import static org.apache.datasketches.server.SketchConstants.QUERY_SKETCH_FIELD;
import static org.apache.datasketches.server.SketchConstants.SKETCH_FAMILY_CPC;
import static org.apache.datasketches.server.SketchConstants.SKETCH_FAMILY_FREQUENCY;
import static org.apache.datasketches.server.SketchConstants.SKETCH_FAMILY_HLL;
import static org.apache.datasketches.server.SketchConstants.SKETCH_FAMILY_KLL;
import static org.apache.datasketches.server.SketchConstants.SKETCH_FAMILY_RESERVOIR;
import static org.apache.datasketches.server.SketchConstants.SKETCH_FAMILY_THETA;
import static org.apache.datasketches.server.SketchConstants.SKETCH_FAMILY_VAROPT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Base64;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.datasketches.ArrayOfStringsSerDe;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.cpc.CpcUnion;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.sampling.ReservoirItemsSketch;
import org.apache.datasketches.sampling.ReservoirItemsUnion;
import org.apache.datasketches.sampling.VarOptItemsSketch;
import org.apache.datasketches.sampling.VarOptItemsUnion;
import org.apache.datasketches.theta.Union;
import org.eclipse.jetty.http.HttpStatus;
import org.testng.annotations.Test;


import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class MergeHandlerTest extends ServerTestBase {
  /* Using knowledge of the implementation of merging, much of the merging code is generic
   * and shared across sketch types. These tests will cover some features only with a single
   * sketch type (theta) in order to avoid lots of repetitive tests, without sacrificing
   * functional coverage.
   */

  @Test
  public void errorMerges() {
    final JsonObject response = new JsonObject();
    JsonObject request = new JsonObject();

    // completely empty request cannot be handled
    assertEquals(postData(MERGE_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);

    // invalid name field with real sketch name
    request.addProperty("notAName", "theta0");
    assertEquals(postData(MERGE_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);

    // valid name, empty value
    request = new JsonObject();
    request.add(QUERY_MERGE_TGT_FIELD, new JsonObject());
    assertEquals(postData(MERGE_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);

    // valid "name" field, invalid sketch name
    request = new JsonObject();
    request.addProperty(QUERY_MERGE_TGT_FIELD, "sketchDoesNotExist");
    assertEquals(postData(MERGE_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);

    // no name, k is not a number
    request = new JsonObject();
    request.addProperty(QUERY_MERGE_K_FIELD, "isNotNumber");

    // valid name with real sketch, no source sketches
    request.addProperty(QUERY_MERGE_TGT_FIELD, "theta2");
    assertEquals(postData(MERGE_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);

    // valid name with real sketch, source is not JsonArray
    request.addProperty(QUERY_MERGE_SRC_FIELD, "notAnArray");
    assertEquals(postData(MERGE_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);

    // serialized input sketch missing family type
    JsonArray inputSketches = new JsonArray(1);
    JsonObject sketchInfo = new JsonObject();
    final CpcSketch cpc = new CpcSketch(10);
    sketchInfo.addProperty(QUERY_DATA_FIELD, Base64.getUrlEncoder().encodeToString(cpc.toByteArray()));
    inputSketches.add(sketchInfo);
    request.add(QUERY_MERGE_SRC_FIELD, inputSketches);
    assertEquals(postData(MERGE_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);

    // serialized input sketch missing value
    inputSketches = new JsonArray(1);
    sketchInfo = new JsonObject();
    sketchInfo.addProperty(QUERY_FAMILY_FIELD, SKETCH_FAMILY_CPC);
    inputSketches.add(sketchInfo);
    request.add(QUERY_MERGE_SRC_FIELD, inputSketches);
    assertEquals(postData(MERGE_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);

    // serialized input with conflicting sketch types
    inputSketches = new JsonArray(1);
    sketchInfo.addProperty(QUERY_DATA_FIELD, Base64.getUrlEncoder().encodeToString(cpc.toByteArray()));
    inputSketches.add(sketchInfo);
    request.add(QUERY_MERGE_SRC_FIELD, inputSketches);
    assertEquals(postData(MERGE_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);

    // input as existing sketch of wrong family
    inputSketches = new JsonArray(1);
    inputSketches.add("cpcOfLongs");
    request.add(QUERY_MERGE_SRC_FIELD, inputSketches);
    assertEquals(postData(MERGE_PATH, request, response), HttpStatus.UNPROCESSABLE_ENTITY_422);
  }

  @Test
  public void cpcMerge() {
    final String tgtName = "cpcOfLongs";
    final JsonObject response = new JsonObject();
    final JsonObject request = new JsonObject();

    // add data to a couple sketches that we'll pass in via serialization
    final int n = 5000;
    final int offset = n / 2;
    final int k = server_.getSketch(tgtName).configK_;
    final CpcSketch sk1 = new CpcSketch(k);
    final CpcSketch sk2 = new CpcSketch(k + 1);
    for (int i = 0; i < n; ++i) {
      sk1.update(i);
      sk2.update(offset + i);
    }
    final CpcUnion union = new CpcUnion(k);
    union.update(sk1);
    union.update(sk2);
    final byte[] tgtBytes = union.getResult().toByteArray();

    // add sketches to the request
    final JsonArray srcSketches = new JsonArray(2);

    final JsonObject serializedSketch1 = new JsonObject();
    serializedSketch1.addProperty(QUERY_FAMILY_FIELD, SKETCH_FAMILY_CPC);
    serializedSketch1.addProperty(QUERY_DATA_FIELD, Base64.getUrlEncoder().encodeToString(sk1.toByteArray()));
    srcSketches.add(serializedSketch1);

    final JsonObject serializedSketch2 = new JsonObject();
    serializedSketch2.addProperty(QUERY_FAMILY_FIELD, SKETCH_FAMILY_CPC);
    serializedSketch2.addProperty(QUERY_DATA_FIELD, Base64.getUrlEncoder().encodeToString(sk2.toByteArray()));
    srcSketches.add(serializedSketch2);
    request.add(QUERY_MERGE_SRC_FIELD, srcSketches);

    // serialized sketch, so set the k value
    request.addProperty(QUERY_MERGE_K_FIELD, k);

    assertEquals(postData(MERGE_PATH, request, response), HttpStatus.OK_200);
    final JsonObject responseData = response.getAsJsonObject(RESPONSE_FIELD).getAsJsonObject();
    assertTrue(responseData.has(QUERY_SKETCH_FIELD));
    assertEquals(Base64.getUrlDecoder().decode(responseData.get(QUERY_SKETCH_FIELD).getAsString()), tgtBytes);

    // save into a target
    request.addProperty(QUERY_MERGE_TGT_FIELD, tgtName);
    assertEquals(postData(MERGE_PATH, request, response), HttpStatus.OK_200);
    assertTrue(response.get(RESPONSE_FIELD).isJsonNull());
    assertEquals(((CpcSketch) server_.getSketch(tgtName).sketch_).toByteArray(), tgtBytes);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void fiMerge() {
    final String tgtName = "topItems";
    final JsonObject response = new JsonObject();
    final JsonObject request = new JsonObject();

    // add data to a couple sketches that we'll pass in via serialization
    final int n = 25;
    final int offset = 5;
    final int k = server_.getSketch(tgtName).configK_;
    final ItemsSketch<String> sk1 = new ItemsSketch<>(k * 2);
    final ItemsSketch<String> sk2 = new ItemsSketch<>(k);
    for (int i = 1; i <= n; ++i) {
      sk1.update(Integer.toString(i), i * i);
      sk2.update(Integer.toString(offset + i), (n - i) * (n - i));
    }
    final ItemsSketch<String> union = new ItemsSketch<>(k);
    union.merge(sk1);
    union.merge(sk2);
    final byte[] tgtBytes = union.toByteArray(new ArrayOfStringsSerDe());

    // add sketches to the request
    final JsonArray srcSketches = new JsonArray(2);

    final JsonObject serializedSketch1 = new JsonObject();
    serializedSketch1.addProperty(QUERY_FAMILY_FIELD, SKETCH_FAMILY_FREQUENCY);
    serializedSketch1.addProperty(QUERY_DATA_FIELD, Base64.getUrlEncoder().encodeToString(sk1.toByteArray(new ArrayOfStringsSerDe())));
    srcSketches.add(serializedSketch1);

    final JsonObject serializedSketch2 = new JsonObject();
    serializedSketch2.addProperty(QUERY_FAMILY_FIELD, SKETCH_FAMILY_FREQUENCY);
    serializedSketch2.addProperty(QUERY_DATA_FIELD, Base64.getUrlEncoder().encodeToString(sk2.toByteArray(new ArrayOfStringsSerDe())));
    srcSketches.add(serializedSketch2);
    request.add(QUERY_MERGE_SRC_FIELD, srcSketches);

    // serialized sketch, so set the k value
    request.addProperty(QUERY_MERGE_K_FIELD, k);

    assertEquals(postData(MERGE_PATH, request, response), HttpStatus.OK_200);
    final JsonObject responseData = response.getAsJsonObject(RESPONSE_FIELD).getAsJsonObject();
    assertTrue(responseData.has(QUERY_SKETCH_FIELD));
    assertEquals(Base64.getUrlDecoder().decode(responseData.get(QUERY_SKETCH_FIELD).getAsString()), tgtBytes);

    // save into a target
    request.addProperty(QUERY_MERGE_TGT_FIELD, tgtName);
    assertEquals(postData(MERGE_PATH, request, response), HttpStatus.OK_200);
    assertTrue(response.get(RESPONSE_FIELD).isJsonNull());
    assertEquals(((ItemsSketch<String>) server_.getSketch(tgtName).sketch_).toByteArray(new ArrayOfStringsSerDe()), tgtBytes);
  }

  @Test
  public void hllMerge() {
    final String tgtName = "hll3";
    final JsonObject response = new JsonObject();
    final JsonObject request = new JsonObject();

    // add data to a couple sketches that we'll pass in via serialization
    final int n = 7500;
    final int offset = n / 3;
    final int k = server_.getSketch(tgtName).configK_;
    final HllSketch sk1 = (HllSketch) server_.getSketch("hll1").sketch_;
    final HllSketch sk2 = (HllSketch) server_.getSketch("hll2").sketch_;
    for (int i = 0; i < n; ++i) {
      sk1.update(i);
      sk2.update(offset + i);
    }
    final org.apache.datasketches.hll.Union union = new org.apache.datasketches.hll.Union(k);
    union.update(sk1);
    union.update(sk2);
    final byte[] tgtBytes = union.getResult().toCompactByteArray();

    // add sketches to the request
    final JsonArray srcSketches = new JsonArray(2);
    final JsonObject serializedSketch = new JsonObject();
    serializedSketch.addProperty(QUERY_FAMILY_FIELD, SKETCH_FAMILY_HLL);
    serializedSketch.addProperty(QUERY_DATA_FIELD, Base64.getUrlEncoder().encodeToString(sk2.toCompactByteArray()));
    srcSketches.add(serializedSketch);
    srcSketches.add("hll1");
    request.add(QUERY_MERGE_SRC_FIELD, srcSketches);

    // serialized sketch, so set the k value
    request.addProperty(QUERY_MERGE_K_FIELD, k);

    assertEquals(postData(MERGE_PATH, request, response), HttpStatus.OK_200);
    final JsonObject responseData = response.getAsJsonObject(RESPONSE_FIELD).getAsJsonObject();
    assertTrue(responseData.has(QUERY_SKETCH_FIELD));
    assertEquals(Base64.getUrlDecoder().decode(responseData.get(QUERY_SKETCH_FIELD).getAsString()), tgtBytes);

    // save into a target
    request.remove(QUERY_MERGE_K_FIELD);
    request.addProperty(QUERY_MERGE_TGT_FIELD, tgtName);
    assertEquals(postData(MERGE_PATH, request, response), HttpStatus.OK_200);
    assertTrue(response.get(RESPONSE_FIELD).isJsonNull());
    assertEquals(((HllSketch) server_.getSketch(tgtName).sketch_).toCompactByteArray(), tgtBytes);

  }

  @Test
  public void kllMerge() {
    final String tgtName = "duration";
    final JsonObject response = new JsonObject();
    final JsonObject request = new JsonObject();

    // add data to a couple sketches that we'll pass in via serialization
    final int n = 6000;
    final int k = server_.getSketch(tgtName).configK_;
    final KllFloatsSketch sk1 = new KllFloatsSketch(k + 20);
    final KllFloatsSketch sk2 = new KllFloatsSketch(k);
    final ThreadLocalRandom rand = ThreadLocalRandom.current();
    for (int i = 1; i <= n; ++i) {
      // unequal number of input points for the sketches
      sk1.update((float) rand.nextGaussian());
      sk2.update((float) rand.nextGaussian() + 2);
      sk2.update((float) rand.nextGaussian() + 3);
    }
    final KllFloatsSketch union = new KllFloatsSketch(k);
    union.merge(sk1);
    union.merge(sk2);

    // add sketches to the request
    final JsonArray srcSketches = new JsonArray(2);

    final JsonObject serializedSketch1 = new JsonObject();
    serializedSketch1.addProperty(QUERY_FAMILY_FIELD, SKETCH_FAMILY_KLL);
    serializedSketch1.addProperty(QUERY_DATA_FIELD, Base64.getUrlEncoder().encodeToString(sk1.toByteArray()));
    srcSketches.add(serializedSketch1);

    final JsonObject serializedSketch2 = new JsonObject();
    serializedSketch2.addProperty(QUERY_FAMILY_FIELD, SKETCH_FAMILY_KLL);
    serializedSketch2.addProperty(QUERY_DATA_FIELD, Base64.getUrlEncoder().encodeToString(sk2.toByteArray()));
    srcSketches.add(serializedSketch2);
    request.add(QUERY_MERGE_SRC_FIELD, srcSketches);

    // serialized sketch, so set the k value
    request.addProperty(QUERY_MERGE_K_FIELD, k);

    assertEquals(postData(MERGE_PATH, request, response), HttpStatus.OK_200);
    final JsonObject responseData = response.getAsJsonObject(RESPONSE_FIELD).getAsJsonObject();
    assertTrue(responseData.has(QUERY_SKETCH_FIELD));
    byte[] skBytes =  Base64.getUrlDecoder().decode(responseData.get(QUERY_SKETCH_FIELD).getAsString());
    KllFloatsSketch rebuilt = KllFloatsSketch.heapify(Memory.wrap(skBytes));
    assertEquals(rebuilt.getMinValue(), union.getMinValue());
    assertEquals(rebuilt.getMaxValue(), union.getMaxValue());
    assertEquals(rebuilt.getN(), union.getN());

    // save into a target
    request.addProperty(QUERY_MERGE_TGT_FIELD, tgtName);
    assertEquals(postData(MERGE_PATH, request, response), HttpStatus.OK_200);
    assertTrue(response.get(RESPONSE_FIELD).isJsonNull());
    skBytes =  Base64.getUrlDecoder().decode(responseData.get(QUERY_SKETCH_FIELD).getAsString());
    rebuilt = KllFloatsSketch.heapify(Memory.wrap(skBytes));
    assertEquals(rebuilt.getMinValue(), union.getMinValue());
    assertEquals(rebuilt.getMaxValue(), union.getMaxValue());
    assertEquals(rebuilt.getN(), union.getN());
  }

  @Test
  public void thetaMerge() {
    final String tgtName = "theta2";
    final JsonObject response = new JsonObject();
    final JsonObject request = new JsonObject();

    // add data to multiple sketches, including one passed in serialized
    final int n = 5;
    int idx = 0;

    final int k = server_.getSketch("theta0").configK_;
    Union sk = (Union) server_.getSketch("theta0").sketch_;
    final Union tgtUnion = Union.builder().setNominalEntries(1 << k).buildUnion();
    for (int i = 0; i < n; ++i) {
      sk.update(idx++);
    }
    tgtUnion.union(sk.getResult());

    sk = (Union) server_.getSketch("theta1").sketch_;
    for (int i = 0; i < n; ++i) {
      sk.update(idx++);
    }
    tgtUnion.union(sk.getResult());

    sk = Union.builder().setNominalEntries(1 << k).buildUnion();
    for (int i = 0; i < n; ++i) {
      sk.update(idx++);
    }
    final String skEncoded = Base64.getUrlEncoder().encodeToString(sk.getResult().toByteArray());

    // we'll compare the result vs a reference serialized sketch
    tgtUnion.union(sk.getResult());
    final byte[] tgtBytes = tgtUnion.getResult().toByteArray();

    final JsonArray srcSketches = new JsonArray(3);
    srcSketches.add("theta0");
    srcSketches.add("theta1");
    final JsonObject serializedSketch = new JsonObject();
    serializedSketch.addProperty(QUERY_FAMILY_FIELD, SKETCH_FAMILY_THETA);
    serializedSketch.addProperty(QUERY_DATA_FIELD, skEncoded);
    srcSketches.add(serializedSketch);
    request.add(QUERY_MERGE_SRC_FIELD, srcSketches);

    // serialized sketch, so set the k value
    request.addProperty(QUERY_MERGE_K_FIELD, k);

    assertEquals(postData(MERGE_PATH, request, response), HttpStatus.OK_200);
    final JsonObject responseData = response.getAsJsonObject(RESPONSE_FIELD).getAsJsonObject();
    assertTrue(responseData.has(QUERY_SKETCH_FIELD));
    assertEquals(Base64.getUrlDecoder().decode(responseData.get(QUERY_SKETCH_FIELD).getAsString()), tgtBytes);

    // save into a target
    request.addProperty(QUERY_MERGE_TGT_FIELD, tgtName);
    assertEquals(postData(MERGE_PATH, request, response), HttpStatus.OK_200);
    assertTrue(response.get(RESPONSE_FIELD).isJsonNull());
    assertEquals(((Union) server_.getSketch(tgtName).sketch_).getResult().toByteArray(), tgtBytes);
  }

  @Test
  public void reservoirMerge() {
    final String tgtName = "rs";
    final JsonObject response = new JsonObject();
    final JsonObject request = new JsonObject();

    // add data to a couple sketches that we'll pass in via serialization
    final int k = server_.getSketch(tgtName).configK_;
    final int n = (k - 1) / 2;
    final ReservoirItemsSketch<String> sk1 = ReservoirItemsSketch.newInstance(k);
    final ReservoirItemsSketch<String> sk2 = ReservoirItemsSketch.newInstance(k);
    for (int i = 0; i < n; ++i) {
      sk1.update(Integer.toString(i));
      sk2.update(Integer.toString(n + i));
    }
    final ReservoirItemsUnion<String> union = ReservoirItemsUnion.newInstance(k);
    union.update(sk1);
    union.update(sk2);
    final ReservoirItemsSketch<String> result = union.getResult();

    // add sketches to the request
    final JsonArray srcSketches = new JsonArray(2);

    final JsonObject serializedSketch1 = new JsonObject();
    serializedSketch1.addProperty(QUERY_FAMILY_FIELD, SKETCH_FAMILY_RESERVOIR);
    serializedSketch1.addProperty(QUERY_DATA_FIELD, Base64.getUrlEncoder().encodeToString(sk1.toByteArray(new ArrayOfStringsSerDe())));
    srcSketches.add(serializedSketch1);

    final JsonObject serializedSketch2 = new JsonObject();
    serializedSketch2.addProperty(QUERY_FAMILY_FIELD, SKETCH_FAMILY_RESERVOIR);
    serializedSketch2.addProperty(QUERY_DATA_FIELD, Base64.getUrlEncoder().encodeToString(sk2.toByteArray(new ArrayOfStringsSerDe())));
    srcSketches.add(serializedSketch2);
    request.add(QUERY_MERGE_SRC_FIELD, srcSketches);

    // serialized sketch, so set the k value
    request.addProperty(QUERY_MERGE_K_FIELD, k);

    assertEquals(postData(MERGE_PATH, request, response), HttpStatus.OK_200);
    final JsonObject responseData = response.getAsJsonObject(RESPONSE_FIELD).getAsJsonObject();
    assertTrue(responseData.has(QUERY_SKETCH_FIELD));
    byte[] skBytes =  Base64.getUrlDecoder().decode(responseData.get(QUERY_SKETCH_FIELD).getAsString());
    ReservoirItemsSketch<String> rebuilt = ReservoirItemsSketch.heapify(Memory.wrap(skBytes), new ArrayOfStringsSerDe());
    assertEquals(rebuilt.getN(), result.getN());
    assertEquals(rebuilt.estimateSubsetSum(x -> true).getTotalSketchWeight(),
        result.estimateSubsetSum(x -> true).getTotalSketchWeight());

    // save into a target
    request.addProperty(QUERY_MERGE_TGT_FIELD, tgtName);
    assertEquals(postData(MERGE_PATH, request, response), HttpStatus.OK_200);
    assertTrue(response.get(RESPONSE_FIELD).isJsonNull());
    skBytes =  Base64.getUrlDecoder().decode(responseData.get(QUERY_SKETCH_FIELD).getAsString());
    rebuilt = ReservoirItemsSketch.heapify(Memory.wrap(skBytes), new ArrayOfStringsSerDe());
    assertEquals(rebuilt.getN(), result.getN());
    assertEquals(rebuilt.estimateSubsetSum(x -> true).getTotalSketchWeight(),
        result.estimateSubsetSum(x -> true).getTotalSketchWeight());
  }

  @Test
  public void varOptMerge() {
    final String tgtName = "vo";
    final JsonObject response = new JsonObject();
    final JsonObject request = new JsonObject();

    // add data to a couple sketches that we'll pass in via serialization
    final int k = server_.getSketch(tgtName).configK_;
    final int n = 5 * k;
    final VarOptItemsSketch<String> sk1 = VarOptItemsSketch.newInstance(k);
    final VarOptItemsSketch<String> sk2 = VarOptItemsSketch.newInstance(k);
    final ThreadLocalRandom rand = ThreadLocalRandom.current();
    for (int i = 0; i < n; ++i) {
      sk1.update(Integer.toString(i), rand.nextInt(25000));
      sk2.update(Integer.toString(n + i), rand.nextInt(5000));
    }
    final VarOptItemsUnion<String> union = VarOptItemsUnion.newInstance(k);
    union.update(sk1);
    union.update(sk2);
    final VarOptItemsSketch<String> result = union.getResult();

    // add sketches to the request
    final JsonArray srcSketches = new JsonArray(2);

    final JsonObject serializedSketch1 = new JsonObject();
    serializedSketch1.addProperty(QUERY_FAMILY_FIELD, SKETCH_FAMILY_VAROPT);
    serializedSketch1.addProperty(QUERY_DATA_FIELD, Base64.getUrlEncoder().encodeToString(sk1.toByteArray(new ArrayOfStringsSerDe())));
    srcSketches.add(serializedSketch1);

    final JsonObject serializedSketch2 = new JsonObject();
    serializedSketch2.addProperty(QUERY_FAMILY_FIELD, SKETCH_FAMILY_VAROPT);
    serializedSketch2.addProperty(QUERY_DATA_FIELD, Base64.getUrlEncoder().encodeToString(sk2.toByteArray(new ArrayOfStringsSerDe())));
    srcSketches.add(serializedSketch2);
    request.add(QUERY_MERGE_SRC_FIELD, srcSketches);

    // serialized sketch, so set the k value
    request.addProperty(QUERY_MERGE_K_FIELD, k);

    assertEquals(postData(MERGE_PATH, request, response), HttpStatus.OK_200);
    final JsonObject responseData = response.getAsJsonObject(RESPONSE_FIELD).getAsJsonObject();
    assertTrue(responseData.has(QUERY_SKETCH_FIELD));
    byte[] skBytes =  Base64.getUrlDecoder().decode(responseData.get(QUERY_SKETCH_FIELD).getAsString());
    VarOptItemsSketch<String> rebuilt = VarOptItemsSketch.heapify(Memory.wrap(skBytes), new ArrayOfStringsSerDe());
    assertEquals(rebuilt.getN(), result.getN());
    assertEquals(rebuilt.estimateSubsetSum(x -> true).getTotalSketchWeight(),
        result.estimateSubsetSum(x -> true).getTotalSketchWeight());

    // save into a target
    request.addProperty(QUERY_MERGE_TGT_FIELD, tgtName);
    assertEquals(postData(MERGE_PATH, request, response), HttpStatus.OK_200);
    assertTrue(response.get(RESPONSE_FIELD).isJsonNull());
    skBytes =  Base64.getUrlDecoder().decode(responseData.get(QUERY_SKETCH_FIELD).getAsString());
    rebuilt = VarOptItemsSketch.heapify(Memory.wrap(skBytes), new ArrayOfStringsSerDe());
    assertEquals(rebuilt.getN(), result.getN());
    assertEquals(rebuilt.estimateSubsetSum(x -> true).getTotalSketchWeight(),
        result.estimateSubsetSum(x -> true).getTotalSketchWeight());
  }
}
