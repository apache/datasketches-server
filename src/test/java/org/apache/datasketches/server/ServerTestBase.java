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

import static org.apache.datasketches.server.SketchConstants.ERROR_KEY;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.eclipse.jetty.http.HttpStatus;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;


import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ServerTestBase {
  final static String RESPONSE_FIELD = "response";

  SketchServer server_ = null;
  String serverUri_ = null;

  @BeforeClass
  public void launchServer() {
    final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    try {
      server_ = new SketchServer(Objects.requireNonNull(classLoader.getResource("test_config.json")).getFile());
      server_.start();
      serverUri_ = server_.getURI();
    } catch (final Exception e) {
      System.err.println(e.getMessage());
      fail();
    }
  }

  @AfterClass
  public void shutdownServer() {
    if (server_ != null) {
      try {
        server_.stop();
      } catch (final Exception e) {
        fail();
      }
    }
  }

  static void clearResponse(@NonNull final JsonObject response) {
    for (final Map.Entry<String, JsonElement> entry : response.entrySet()) {
      response.remove(entry.getKey());
    }
  }

  int postData(@NonNull final String path,
               @NonNull final JsonObject data,
               @NonNull final JsonObject response) {
    HttpURLConnection http = null;
    int status = -1;
    clearResponse(response);

    try {
      // set up the POST
      final URL url = new URL(serverUri_ + path);
      http = (HttpURLConnection) url.openConnection();
      http.setDoInput(true);
      http.setDoOutput(true);
      http.setRequestMethod("POST");
      http.setRequestProperty("Content-Type", "application/json");
      http.setRequestProperty("Accept", "application/json");

      final byte[] jsonBytes = data.toString().getBytes(StandardCharsets.UTF_8);
      http.setRequestProperty("Content-length", Integer.toString(jsonBytes.length));

      // write JSON data to to stream
      try (final DataOutputStream os = new DataOutputStream(http.getOutputStream())) {
        os.write(jsonBytes);
      }

      status = http.getResponseCode();

      final JsonElement responseData;
      if (status == HttpStatus.OK_200) {
        // read response, if any, and put into a JSON element
        try (final InputStreamReader isr = new InputStreamReader(http.getInputStream())) {
          responseData = JsonParser.parseReader(isr);
        }
        if (!responseData.isJsonNull()) {
          response.add(RESPONSE_FIELD, responseData);
        }
      } else if (status == HttpStatus.UNPROCESSABLE_ENTITY_422) {
        // read error response and put into a JSON element
        try (final InputStreamReader isr = new InputStreamReader(http.getErrorStream())) {
          responseData = JsonParser.parseReader(isr);
        }
        if (!responseData.isJsonNull()) {
          response.add(ERROR_KEY, responseData);
        }
      }
    } catch (final IOException e) {
        fail();
    } finally {
      if (http != null)
        http.disconnect();
    }

    return status;
  }

  int getData(@NonNull final String path,
              @NonNull final JsonObject data,
              @NonNull final JsonObject response) {
    HttpURLConnection http = null;
    int status = -1;
    clearResponse(response);

    try {
      // set up the GET
      final URL url = new URL(serverUri_ + path + "?" + data);
      http = (HttpURLConnection) url.openConnection();
      http.setDoInput(true);
      http.setRequestProperty("Content-Type", "application/json");
      http.connect();

      status = http.getResponseCode();

      final JsonElement responseData;
      if (status == HttpStatus.OK_200) {
        // read response, if any, and put into a JSON element
        try (final InputStreamReader isr = new InputStreamReader(http.getInputStream())) {
          responseData = JsonParser.parseReader(isr);
        }
        if (!responseData.isJsonNull()) {
          response.add(RESPONSE_FIELD, responseData);
        }
      } else if (status == HttpStatus.UNPROCESSABLE_ENTITY_422) {
        // read error response and put into a JSON element
        try (final InputStreamReader isr = new InputStreamReader(http.getErrorStream())) {
          responseData = JsonParser.parseReader(isr);
        }
        if (!responseData.isJsonNull()) {
          response.add(ERROR_KEY, responseData);
        }
      }
    } catch (final IOException e) {
      fail();
    } finally {
      if (http != null)
        http.disconnect();
    }

    return status;
  }
}
