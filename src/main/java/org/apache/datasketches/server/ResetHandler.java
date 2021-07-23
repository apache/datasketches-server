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


import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.sampling.ReservoirItemsSketch;
import org.apache.datasketches.sampling.VarOptItemsSketch;
import org.apache.datasketches.theta.Union;


import com.google.gson.JsonObject;

public class ResetHandler extends BaseSketchesQueryHandler {
  ResetHandler(final SketchStorage sketches) {
    super(sketches, false);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected JsonObject processQuery(final JsonObject query) {
    if (!query.has(QUERY_NAME_FIELD)) {
      throw new IllegalArgumentException("Query missing sketch name field");
    }

    final String key = query.get(QUERY_NAME_FIELD).getAsString();
    if (!sketches.contains(key)) {
      throw new IllegalArgumentException("Invalid sketch name: " + key);
    }

    synchronized (key.intern()) {
      final SketchStorage.SketchEntry se = sketches.getSketch(key);

      switch (se.family_) {
        case UNION:
          ((Union) se.sketch_).reset();
          break;
        case KLL:
          se.sketch_ = new KllFloatsSketch(se.configK_);
          break;
        case FREQUENCY:
          ((ItemsSketch<String>) se.sketch_).reset();
          break;
        case HLL:
          ((HllSketch) se.sketch_).reset();
          break;
        case CPC:
          ((CpcSketch) se.sketch_).reset();
          break;
        case RESERVOIR:
          ((ReservoirItemsSketch<String>) se.sketch_).reset();
          break;
        case VAROPT:
          ((VarOptItemsSketch<String>) se.sketch_).reset();
          break;
      }
    }

    // nothing to return from reset calls
    return null;
  }

}
