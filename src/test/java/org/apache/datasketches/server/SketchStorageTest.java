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

import static org.apache.datasketches.server.SketchConstants.RESPONSE_SKETCH_COUNT_FIELD;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.util.Objects;

import org.apache.datasketches.Family;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.hll.HllSketch;
import org.testng.annotations.Test;


import com.google.gson.JsonObject;

public class SketchStorageTest {

  @Test
  public void invalidSketchEntry() {
    try {
      new SketchStorage.SketchEntry(Family.CPC, null, new CpcSketch(12), "cpcSketch", 12);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      new SketchStorage.SketchEntry(Family.HLL, new HllSketch(10), "hllSketch", 10);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void loadSketches() {
    final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    SketchServerConfig serverConfig = null;
    try {
      serverConfig = new SketchServerConfig(Objects.requireNonNull(classLoader.getResource("test_config.json")).getFile());
    } catch (final IOException e) {
      fail();
    }
    assertNotNull(serverConfig);

    final SketchStorage storage = new SketchStorage(serverConfig.getSketchList());
    final JsonObject sketches = storage.listSketches();
    assertTrue(sketches.has(RESPONSE_SKETCH_COUNT_FIELD));
    assertEquals(sketches.get(RESPONSE_SKETCH_COUNT_FIELD).getAsInt(), 20);
    assertTrue(storage.contains("cpcOfFloats"));
  }
}
