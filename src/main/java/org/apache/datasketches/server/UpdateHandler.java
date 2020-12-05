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

import java.util.Map;

import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.sampling.ReservoirItemsSketch;
import org.apache.datasketches.sampling.VarOptItemsSketch;
import org.apache.datasketches.theta.Union;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import static org.apache.datasketches.server.SketchConstants.*;

/**
 *
 * <p>
 * Update format (single- and multi-value updates):
 * <pre>
 * {
 *   "&lt;sketch_name_1&gt;": &lt;value&gt;,
 *   "&lt;sketch_name_2&gt;": [&lt;value_1&gt;, &lt;value_2&gt;, ...].
 *   ...
 * }
 * </pre>
 * where the type of each value is determined by the sketch configuration at server initialization.
 * </p>
 * <p>
 * Each <em>value</em> may be a stand-alone item or, for sketches accepting weighted inputs, may be a JSON
 * object: <tt>{ "item": &lt;value&gt;, "weight": &lt;weight&gt; }</tt>. Attempting to use a weight with a
 * sketch that does not support weighted inputs will result in an error. The multi-value update format
 * may contain a mux of weighted and unweighted items, as lonig s the sketch accepts weighted values.
 * </p>
 * <p>
 * The JSON parsing library does not allow duplicate key names; to send multiple values into the same sketch
 * in a single request, the multi-value option <em>must</em> be used.
 * </p>
 * <p>
 * This is not a transactional database: Entries are processed and committed sequentially, returning an error
 * if one is encountered at any point in the process. Any updates processed prior to the error will be
 * retained by the server.
 * </p>
 */
public class UpdateHandler extends BaseSketchesQueryHandler {
  public UpdateHandler(SketchStorage sketches) {
    super(sketches);
  }

  @Override
  protected JsonObject processQuery(JsonObject query) {
    for (Map.Entry<String, JsonElement> entry : query.entrySet()) {
      String name = entry.getKey();
      SketchStorage.SketchEntry se = sketches.getSketch(name);
      JsonElement data = entry.getValue();

      if (name == null || se == null || data == null) {
        throw new IllegalArgumentException("Attempt to call update with missing name or sketch not found");
      }

      if (data.isJsonArray()) {
        processBatchUpdate(se, data.getAsJsonArray());
      } else {
        processSingleUpdate(se, data);
      }
    }

    // nothing to return from update
    return null;
  }

  @SuppressWarnings("unchecked")
  private void processBatchUpdate(SketchStorage.SketchEntry entry,
                                  JsonArray data) {
    switch (entry.family) {
      case UNION: // theta
        assert(entry.type != null);
        switch (entry.type) {
          case FLOAT: case DOUBLE:
            for (JsonElement e : data) { ((Union) entry.sketch).update(e.getAsDouble()); }
            break;
          case INT: case LONG:
            for (JsonElement e : data) { ((Union) entry.sketch).update(e.getAsLong()); }
            break;
          case STRING: default:
            for (JsonElement e : data) { ((Union) entry.sketch).update(e.getAsString()); }
            break;
        }
        break;

      case CPC:
        assert(entry.type != null);
        switch (entry.type) {
          case FLOAT: case DOUBLE:
            for (JsonElement e : data) { ((CpcSketch) entry.sketch).update(e.getAsDouble()); }
            break;
          case INT: case LONG:
            for (JsonElement e : data) { ((CpcSketch) entry.sketch).update(e.getAsLong()); }
            break;
          case STRING: default:
            for (JsonElement e : data) { ((CpcSketch) entry.sketch).update(e.getAsString()); }
            break;
        }
        break;

      case HLL:
        assert(entry.type != null);
        switch (entry.type) {
          case FLOAT: case DOUBLE:
            for (JsonElement e : data) { ((HllSketch) entry.sketch).update(e.getAsDouble()); }
            break;
          case INT: case LONG:
            for (JsonElement e : data) { ((CpcSketch) entry.sketch).update(e.getAsLong()); }
            break;
          case STRING: default:
            for (JsonElement e : data) { ((CpcSketch) entry.sketch).update(e.getAsString()); }
            break;
        }
        break;

      case KLL:
        for (JsonElement e : data) { ((KllFloatsSketch) entry.sketch).update(e.getAsFloat()); }
        break;

      case FREQUENCY:
        for (JsonElement e : data) {
          if (e.isJsonObject()) {
            JsonObject inputPair = e.getAsJsonObject();
            if (!inputPair.has(QUERY_PAIR_ITEM_FIELD) || !inputPair.has(QUERY_PAIR_WEIGHT_FIELD)) {
              throw new IllegalArgumentException("Frequent Items input pairs must include both "
                  + QUERY_PAIR_ITEM_FIELD + " and " + QUERY_PAIR_WEIGHT_FIELD + " values");
            }
            String item = inputPair.get(QUERY_PAIR_ITEM_FIELD).getAsString();
            int weight = inputPair.get(QUERY_PAIR_WEIGHT_FIELD).getAsInt();
            ((ItemsSketch<String>) entry.sketch).update(item, weight);
          } else {
            ((ItemsSketch<String>) entry.sketch).update(e.getAsString());
          }
        }
        break;

      case RESERVOIR:
        for (JsonElement e : data) { ((ReservoirItemsSketch<String>) entry.sketch).update(e.getAsString()); }
        break;

      case VAROPT:
        for (JsonElement e : data) {
          if (e.isJsonObject()) {
            JsonObject inputPair = e.getAsJsonObject();
            if (!inputPair.has(QUERY_PAIR_ITEM_FIELD) || !inputPair.has(QUERY_PAIR_WEIGHT_FIELD)) {
              throw new IllegalArgumentException("VarOpt input pairs must include both "
                  + QUERY_PAIR_ITEM_FIELD + " and " + QUERY_PAIR_WEIGHT_FIELD + " values");
            }
            String item = inputPair.get(QUERY_PAIR_ITEM_FIELD).getAsString();
            double weight = inputPair.get(QUERY_PAIR_WEIGHT_FIELD).getAsDouble();
            ((VarOptItemsSketch<String>) entry.sketch).update(item, weight);
          } else {
            ((VarOptItemsSketch<String>) entry.sketch).update(e.getAsString(), 1.0);
          }
        }
        break;

      default:
        throw new IllegalArgumentException("Unsupported sketch type: " + entry.family);
    }
  }

  @SuppressWarnings("unchecked")
  private void processSingleUpdate(SketchStorage.SketchEntry entry,
                                   JsonElement data) {
    switch (entry.family) {
      case UNION:
        assert(entry.type != null);
        switch (entry.type) {
          case FLOAT: case DOUBLE:
            ((Union) entry.sketch).update(data.getAsDouble());
            break;
          case INT: case LONG:
            ((Union) entry.sketch).update(data.getAsLong());
            break;
          case STRING: default:
            ((Union) entry.sketch).update(data.getAsString());
            break;
        }
        break;

      case CPC:
        assert(entry.type != null);
        switch (entry.type) {
          case FLOAT: case DOUBLE:
            ((CpcSketch) entry.sketch).update(data.getAsDouble());
            break;
          case INT: case LONG:
            ((CpcSketch) entry.sketch).update(data.getAsLong());
            break;
          case STRING: default:
            ((CpcSketch) entry.sketch).update(data.getAsString());
            break;
        }
        break;

      case HLL:
        assert(entry.type != null);
        switch (entry.type) {
          case FLOAT: case DOUBLE:
            ((HllSketch) entry.sketch).update(data.getAsDouble());
            break;
          case INT: case LONG:
            ((HllSketch) entry.sketch).update(data.getAsLong());
            break;
          case STRING: default:
            ((HllSketch) entry.sketch).update(data.getAsString());
            break;
        }
        break;

      case KLL:
        ((KllFloatsSketch) entry.sketch).update(data.getAsFloat());
        break;

      case FREQUENCY:
        if (data.isJsonObject()) {
          JsonObject inputPair = data.getAsJsonObject();
          if (!inputPair.has(QUERY_PAIR_ITEM_FIELD) || !inputPair.has(QUERY_PAIR_WEIGHT_FIELD)) {
            throw new IllegalArgumentException("Frequent Items input pairs must include both "
                + QUERY_PAIR_ITEM_FIELD + " and " + QUERY_PAIR_WEIGHT_FIELD + " values");
          }
          String item = inputPair.get(QUERY_PAIR_ITEM_FIELD).getAsString();
          int weight = inputPair.get(QUERY_PAIR_WEIGHT_FIELD).getAsInt();
          ((ItemsSketch<String>) entry.sketch).update(item, weight);
        } else {
          ((ItemsSketch<String>) entry.sketch).update(data.getAsString());
        }
        break;

      case RESERVOIR:
        ((ReservoirItemsSketch<String>) entry.sketch).update(data.getAsString());
        break;

      case VAROPT:
        if (data.isJsonObject()) {
          JsonObject inputPair = data.getAsJsonObject();
          if (!inputPair.has(QUERY_PAIR_ITEM_FIELD) || !inputPair.has(QUERY_PAIR_WEIGHT_FIELD)) {
            throw new IllegalArgumentException("VarOpt input pairs must include both "
                + QUERY_PAIR_ITEM_FIELD + " and " + QUERY_PAIR_WEIGHT_FIELD + " values");
          }
          String item = inputPair.get(QUERY_PAIR_ITEM_FIELD).getAsString();
          double weight = inputPair.get(QUERY_PAIR_WEIGHT_FIELD).getAsDouble();
          ((VarOptItemsSketch<String>) entry.sketch).update(item, weight);
        } else {
          ((VarOptItemsSketch<String>) entry.sketch).update(data.getAsString(), 1.0);
        }
        break;

      default:
        throw new IllegalArgumentException("Unsupported sketch type: " + entry.family);
    }
  }

}

