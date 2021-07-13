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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.datasketches.Family;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.sampling.ReservoirItemsSketch;
import org.apache.datasketches.sampling.VarOptItemsSketch;
import org.apache.datasketches.theta.SetOperationBuilder;
import org.checkerframework.checker.nullness.qual.NonNull;


import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import static org.apache.datasketches.server.SketchConstants.*;

/**
 * A storage class for holding sketches. Each sketch must have a unique name, and be instantiated with one
 * of the known types. For some sketch families, a <tt>ValueType</tt> (@see ValueType) may be required in
 * order to ensure that data is presented in a consistent way.
 */
public class SketchStorage {
  // the set of SketchEntries held by this object
  HashMap<String, SketchEntry> sketchMap;

  /**
   * Returns true if the sketch family is for distinct counting.
   * @param family A sketch Family (@see org.apache.datasketches.Family)
   * @return <tt>true</tt>e for distinct counting sketch families, otherwise <tt>false</tt>>.
   */
  static boolean isDistinctCounting(final Family family) {
    return family == Family.QUICKSELECT || family == Family.UNION || family == Family.HLL || family == Family.CPC;
  }

  static class SketchEntry {
    public final Family family;
    public final ValueType type;
    public Object sketch;
    public int configK;

    SketchEntry(final Family family, final ValueType type, final Object sketch, final int configK) throws IllegalArgumentException {
      if (isDistinctCounting(family) && type == null)
        throw new IllegalArgumentException("Must specify a value type for distinct counting sketches");

      this.family = family;
      this.type = type;
      this.sketch = sketch;
      this.configK = configK;
    }

    SketchEntry(final Family family, final Object sketch, final int configK) throws IllegalArgumentException {
      if (isDistinctCounting(family))
        throw new IllegalArgumentException("Must specify a value type for distinct counting sketches");

      this.family = family;
      this.type = null;
      this.sketch = sketch;
      this.configK = configK;
    }
  }

  SketchStorage(@NonNull final List<SketchServerConfig.SketchInfo> sketchList) {
    createSketches(sketchList);
  }

  JsonObject listSketches() {
    final JsonObject summary = new JsonObject();

    final JsonArray sketchList = new JsonArray(sketchMap.size());
    for (final Map.Entry<String, SketchEntry> e : sketchMap.entrySet()) {
      final JsonObject item = new JsonObject();
      item.addProperty(CONFIG_SKETCH_NAME_FIELD, e.getKey());
      switch (e.getValue().family) {
        case UNION:
          item.addProperty(CONFIG_TYPE_FIELD, e.getValue().type.getTypeName());
          item.addProperty(CONFIG_FAMILY_FIELD, SKETCH_FAMILY_THETA);
          break;
        case CPC:
          item.addProperty(CONFIG_TYPE_FIELD, e.getValue().type.getTypeName());
          item.addProperty(CONFIG_FAMILY_FIELD, SKETCH_FAMILY_CPC);
          break;
        case HLL:
          item.addProperty(CONFIG_TYPE_FIELD, e.getValue().type.getTypeName());
          item.addProperty(CONFIG_FAMILY_FIELD, SKETCH_FAMILY_HLL);
          break;
        case FREQUENCY:
          item.addProperty(CONFIG_FAMILY_FIELD, SKETCH_FAMILY_FREQUENCY);
          break;
        case KLL:
          item.addProperty(CONFIG_FAMILY_FIELD, SKETCH_FAMILY_KLL);
          break;
        case RESERVOIR:
          item.addProperty(CONFIG_FAMILY_FIELD, SKETCH_FAMILY_RESERVOIR);
          break;
        case VAROPT:
          item.addProperty(CONFIG_FAMILY_FIELD, SKETCH_FAMILY_VAROPT);
          break;
      }
      sketchList.add(item);
    }

    summary.addProperty(RESPONSE_SKETCH_COUNT_FIELD, sketchMap.size());
    summary.add(SketchConstants.CONFIG_SKETCHES_PREFIX, sketchList); // bare prefix, sketches fully qualified

    return summary;
  }

  boolean contains(final String key) {
     return sketchMap.containsKey(key);
  }

  SketchEntry getSketch(final String key) {
    return sketchMap.get(key);
  }

  // instantiate the actual sketches, throwing if there's a duplicate key
  private void createSketches(final List<SketchServerConfig.SketchInfo> list) throws IllegalArgumentException {
    sketchMap = new HashMap<>(list.size());

    for (final SketchServerConfig.SketchInfo info : list) {
      if (sketchMap.containsKey(info.name)) {
        throw new IllegalArgumentException("Duplicate sketch key: " + info.name);
      }

      SketchEntry sketchEntry = null;
      final Family family = BaseSketchesQueryHandler.familyFromString(info.family);

      switch (family) {
        case QUICKSELECT:
          // make a Union so we can handle merges later
          sketchEntry = new SketchEntry(Family.UNION, ValueType.stringToType(info.type),
              new SetOperationBuilder().setNominalEntries(1 << info.k).buildUnion(), info.k);
          break;

        case HLL:
          sketchEntry = new SketchEntry(Family.HLL, ValueType.stringToType(info.type),
              new HllSketch(info.k), info.k);
          break;

        case CPC:
          sketchEntry = new SketchEntry(Family.CPC, ValueType.stringToType(info.type),
              new CpcSketch(info.k), info.k);
          break;

        case KLL:
          sketchEntry = new SketchEntry(Family.KLL, new KllFloatsSketch(info.k), info.k);
          break;

        case FREQUENCY:
          sketchEntry = new SketchEntry(Family.FREQUENCY, new ItemsSketch<String>(info.k), info.k);
          break;

        case RESERVOIR:
          sketchEntry = new SketchEntry(Family.RESERVOIR, ReservoirItemsSketch.<String>newInstance(info.k), info.k);
          break;

        case VAROPT:
          sketchEntry = new SketchEntry(Family.VAROPT, VarOptItemsSketch.<String>newInstance(info.k), info.k);
          break;
      }

      if (sketchEntry != null) {
        sketchMap.put(info.name, sketchEntry);
      }
    }
  }
}
