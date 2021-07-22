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

import java.util.Base64;

import org.apache.datasketches.ArrayOfStringsSerDe;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.sampling.ReservoirItemsSketch;
import org.apache.datasketches.sampling.VarOptItemsSketch;
import org.apache.datasketches.theta.Union;

import com.google.gson.JsonObject;

import static org.apache.datasketches.server.SketchConstants.*;

/**
 * Returns a serialized image of a sketch, encoded in base64, as well as the sketch family and, if relevant,
 * the <tt>ValueType</tt> used.
 * <pre>
 *   {
 *     "name": "&lt;sketch_name&gt;"
 *   }
 * </pre>
 */
public class SerializationHandler extends BaseSketchesQueryHandler {
  public SerializationHandler(final SketchStorage sketches) {
    super(sketches, false);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected JsonObject processQuery(final JsonObject query) {
    if (!query.has(QUERY_NAME_FIELD)) {
      throw new IllegalArgumentException("Query missing sketch name field");
    }

    final String name = query.get(QUERY_NAME_FIELD).getAsString();
    if (!sketches.contains(name)) {
      throw new IllegalArgumentException("Invalid sketch name: " + name);
    }

    final byte[] bytes;
    final SketchStorage.SketchEntry se;

    // need to lock the sketch even when just reading
    synchronized (name.intern()) {
      se = sketches.getSketch(name);

      switch (se.family_) {
        case UNION:
          bytes = ((Union) se.sketch_).getResult().toByteArray();
          break;
        case KLL:
          bytes = ((KllFloatsSketch) se.sketch_).toByteArray();
          break;
        case FREQUENCY:
          bytes = ((ItemsSketch<String>) se.sketch_).toByteArray(new ArrayOfStringsSerDe());
          break;
        case HLL:
          bytes = ((HllSketch) se.sketch_).toCompactByteArray();
          break;
        case CPC:
          bytes = ((CpcSketch) se.sketch_).toByteArray();
          break;
        case RESERVOIR:
          bytes = ((ReservoirItemsSketch<String>) se.sketch_).toByteArray(new ArrayOfStringsSerDe());
          break;
        case VAROPT:
          bytes = ((VarOptItemsSketch<String>) se.sketch_).toByteArray(new ArrayOfStringsSerDe());
          break;
        default:
          throw new IllegalStateException("Unexpected value: " + se.family_);
      }
    }

    final String b64Sketch = Base64.getUrlEncoder().encodeToString(bytes);

    final JsonObject result = new JsonObject();
    result.addProperty(QUERY_NAME_FIELD, name);
    result.addProperty(CONFIG_FAMILY_FIELD, se.family_.getFamilyName());
    if (se.type_ != null)
      result.addProperty(CONFIG_TYPE_FIELD, se.type_.getTypeName());
    result.addProperty(QUERY_SKETCH_FIELD, b64Sketch);

    return result;
  }
}
