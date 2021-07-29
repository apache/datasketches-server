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
import static org.apache.datasketches.server.SketchConstants.STATUS_PATH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.eclipse.jetty.http.HttpStatus;
import org.testng.annotations.Test;


import com.google.gson.JsonObject;

public class StatusHandlerTest extends ServerTestBase {
  @Test
  public void callStatus() {
    // really simple call -- the parameters are ignored
    final JsonObject response = new JsonObject();
    JsonObject request = new JsonObject();

    // GET will easily work since the header size isn't an issue with no data
    assertEquals(getData(STATUS_PATH, request, response), HttpStatus.OK_200);
    JsonObject data = response.get(RESPONSE_FIELD).getAsJsonObject();
    assertTrue(data.has(RESPONSE_SKETCH_COUNT_FIELD));
    assertEquals(data.get(RESPONSE_SKETCH_COUNT_FIELD).getAsInt(), 20);

    // POST the same query
    request.addProperty("notAName", "theta0");
    assertEquals(postData(STATUS_PATH, request, response), HttpStatus.OK_200);
    data = response.get(RESPONSE_FIELD).getAsJsonObject();
    assertTrue(data.has(RESPONSE_SKETCH_COUNT_FIELD));
    assertEquals(data.get(RESPONSE_SKETCH_COUNT_FIELD).getAsInt(), 20);

    // send in a tiny bit of data that should be ignored
    request = new JsonObject();
    request.add("name", new JsonObject());
    assertEquals(postData(STATUS_PATH, request, response), HttpStatus.OK_200);
    data = response.get(RESPONSE_FIELD).getAsJsonObject();
    assertTrue(data.has(RESPONSE_SKETCH_COUNT_FIELD));
    assertEquals(data.get(RESPONSE_SKETCH_COUNT_FIELD).getAsInt(), 20);
  }
}
