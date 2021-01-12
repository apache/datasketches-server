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

import static org.apache.datasketches.server.SketchStorage.isDistinctCounting;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import static org.apache.datasketches.server.SketchConstants.*;

/**
 * A class to hold the server configuration, along with a supporting subclass and file-parsing methods.
 *
 * TODO: define config options
 */
class SketchServerConfig {
  public static class SketchInfo {
    public String name;
    public int k;
    public String family;
    public String type;

    SketchInfo(final String name, final int k, final String family, final String type) {
      this.name = name;
      this.k = k;
      this.family = family;
      this.type = type;
    }

    SketchInfo(final String name, final int k, final String family) {
      this(name, k, family, null);
    }

  }

  private int port = DEFAULT_PORT;
  private ArrayList<SketchInfo> sketchList;

  SketchServerConfig(final String configFile) throws IOException {
    final JsonElement config = readJsonFromFile(configFile);
    parseConfig(config);
  }

  SketchServerConfig(final JsonElement config) throws IOException {
    parseConfig(config);
  }

  int getPort() {
    return port;
  }

  List<SketchInfo> getSketchList() {
    return sketchList;
  }

  // output should have a list with full info per sketch, even if input allows a
  // more condensed format
  private static JsonElement readJsonFromFile(final String configFile) {
    JsonElement config = null;

    try (final Reader reader = Files.newBufferedReader(Paths.get(configFile))) {
      config = JsonParser.parseReader(reader);
    } catch (final IOException e) {
      e.printStackTrace();
    }

    return config;
  }

  private void parseConfig(final JsonElement config) throws IOException {
    final Gson gson = new Gson();

    sketchList = new ArrayList<>();

    if (config.isJsonArray()) {
      // must be a list of fully-described sketches
      sketchList.addAll(Arrays.asList(gson.fromJson(config.getAsJsonArray(), SketchInfo[].class)));
    } else if (config.isJsonObject()) {
      final JsonObject confEntry = config.getAsJsonObject();
      for (final String name : confEntry.keySet()) {
        if (name.equalsIgnoreCase(CONFIG_PORT_FIELD)) {
          // port the server should use
          port = confEntry.get(name).getAsInt();
        }
        else if (name.toLowerCase().startsWith(CONFIG_SKETCHES_PREFIX)) {
          // sketches* is an array of fully qualified sketches
          sketchList.addAll(Arrays.asList(gson.fromJson(confEntry.get(name).getAsJsonArray(), SketchInfo[].class)));
        } else if (name.toLowerCase().startsWith(CONFIG_SET_PREFIX)) {
          // set* has a common name and type with an array of name names
          final JsonObject sketchSetInfo = confEntry.get(name).getAsJsonObject();
          final int k = sketchSetInfo.get(CONFIG_K_FIELD).getAsInt();
          final String family = sketchSetInfo.get(CONFIG_FAMILY_FIELD).getAsString();
          String type = null;
          if (isDistinctCounting(BaseSketchesQueryHandler.familyFromString(family))) {
            type = sketchSetInfo.get(CONFIG_TYPE_FIELD).getAsString();
          }
          final String[] nameList = gson.fromJson(sketchSetInfo.get(CONFIG_SET_NAMES_FIELD).getAsJsonArray(), String[].class);

          for (final String n : nameList)
            sketchList.add(new SketchInfo(n, k, family, type));
        }
      }
    } else {
      throw new IOException("Expected JsonArray or JsonObject but none found");
    }
  }
}
