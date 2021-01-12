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

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;

import org.apache.datasketches.ArrayOfStringsSerDe;
import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesException;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.cpc.CpcUnion;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.sampling.ReservoirItemsSketch;
import org.apache.datasketches.sampling.ReservoirItemsUnion;
import org.apache.datasketches.sampling.VarOptItemsSketch;
import org.apache.datasketches.sampling.VarOptItemsUnion;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.SetOperationBuilder;
import org.apache.datasketches.theta.Union;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * Performs a merge operation between sketches.
 * <pre>
 *   {
 *     "target": "&lt;destination_key&gt;", // optional: return serialized result if missing
 *     "source": [ { "family": "&lt;sketch_family&gt;",
 *                "data": "&lt;base64_encoded_sketch&gt;"
 *              } ]
 *   }
 * </pre>
 * where <tt>source</tt> is an array of key names or {family, data} pairs. Inputs must be of the same family
 * as the target. If no target is present, the family of the first input sketch is used instead. Merge order
 * is not guaranteed.
 */
public class MergeHandler extends BaseSketchesQueryHandler {
  MergeHandler(final SketchStorage sketches) {
    super(sketches);
  }

  @Override
  protected JsonObject processQuery(final JsonObject query) {
    // optional targets:
    // If no QUERY_MERGE_TGT_FIELD serialize the result, but then need specify QUERY_MERGE_K_FIELD.
    // If a valid target is present, any value of QUERY_MERGE_K_FIELD is ignored
    final JsonElement dstElement = query.get(SketchConstants.QUERY_MERGE_TGT_FIELD);
    final String dst = dstElement != null ? dstElement.getAsString() : null;
    if (dst != null && !sketches.contains(dst)) {
      throw new IllegalArgumentException("Specified target sketch does not exist: " + dst);
    }

    int k = 0;
    if (dst == null) {
      final JsonElement kElement = query.get(SketchConstants.QUERY_MERGE_K_FIELD);
      if (kElement == null) {
        throw new IllegalArgumentException("Must specify either \"" + SketchConstants.QUERY_MERGE_TGT_FIELD
            + "\" or \"" + SketchConstants.QUERY_MERGE_K_FIELD + "\". Neither found.");
      }
      k = kElement.getAsInt();
    }

    final JsonElement srcElement = query.get(SketchConstants.QUERY_MERGE_SRC_FIELD);
    if (srcElement == null || !srcElement.isJsonArray()) {
      throw new IllegalArgumentException("Merge source data must be a JSON Array");
    }
    final JsonArray srcList = srcElement.getAsJsonArray();

    SketchStorage.SketchEntry se = null;
    Family dstFamily = null;
    if (dst != null) {
      se = sketches.getSketch(dst);
      dstFamily = se.family;
    }

    // we'll process (and dedup) any stored sketches before we handle encoded inputs
    // but we'll run through all of them before doing anything
    final ArrayList<Object> srcSketches = new ArrayList<>(srcList.size());

    dstFamily = prepareSketches(srcList, dstFamily, dst, srcSketches);
    final byte[] skBytes = mergeSketches(dstFamily, k, se, srcSketches);

    // skBytes == null if merging into another sketch; only non-null if returning a serialized image
    if (skBytes != null) {
      final JsonObject result = new JsonObject();
      result.addProperty(SketchConstants.QUERY_ENCODING_FIELD, SketchConstants.ENCODING_TYPE);
      result.addProperty(SketchConstants.QUERY_SKETCH_FIELD, Base64.getEncoder().encodeToString(skBytes));
      return result;
    } else {
      return null;
    }
  }

  private Family prepareSketches(final JsonArray sources, Family family, final String dst, final ArrayList<Object> sketchList) {
    final HashSet<String> namedSet = new HashSet<>();

    // TODO: Check for sketch value types with distinct counting?
    //       Less obvious how to handle serialized sketch input in that case

    // add destination if one exists just in case it's also listed as a source
    if (dst != null) {
      namedSet.add(dst);
    }

    for (final JsonElement elmt : sources) {
      if (elmt.isJsonPrimitive()) {
        // check family
        final String key = elmt.getAsString();
        final SketchStorage.SketchEntry entry = sketches.getSketch(key);
        if (entry == null || (family != null && family != entry.family)) {
          throw new SketchesException("Input sketches must exist and be of the same family as the target");
        }

        // add to set, save family if we didn't have one yet
        if (!namedSet.contains(key)) {
          namedSet.add(key);
          if (family == null) {
            family = entry.family;
          }

          // if we have a theta Union we need to get the result first
          if (entry.family == Family.UNION) {
            sketchList.add(((Union) entry.sketch).getResult());
          } else {
            sketchList.add(entry.sketch);
          }
        }
      } else { // is JsonObject
        // need special handling for theta as we store Unions?
        final JsonObject sourceObj = elmt.getAsJsonObject();
        if (!sourceObj.has(SketchConstants.QUERY_FAMILY_FIELD)
            || !sourceObj.has(SketchConstants.QUERY_DATA_FIELD)) {
          throw new SketchesException("Base64 sketch used as merge input must specify both \""
              + SketchConstants.QUERY_FAMILY_FIELD + "\" and \"" + SketchConstants.QUERY_DATA_FIELD + "\"");
        }

        final Family skFamily = familyFromString(sourceObj.get(SketchConstants.QUERY_FAMILY_FIELD).getAsString());
        final String skString = sourceObj.get(SketchConstants.QUERY_DATA_FIELD).getAsString();
        if (skString == null || (family != null && family != skFamily)) {
          throw new SketchesException("Input sketches must exist and be of the same family as the target");
        }

        // add to list, save family if we didn't have one yet
        final Object sketch = deserializeSketch(skFamily, skString);
        sketchList.add(sketch);
        if (family == null) {
          family = skFamily;
        }
      }
    }

    return family;
  }

  private static Object deserializeSketch(final Family family, final String b64String) {
    if (family == null || b64String == null) {
      return null;
    }

    final Memory skBytes = Memory.wrap(Base64.getDecoder().decode(b64String));

    switch (family) {
      case QUICKSELECT:
        return CompactSketch.heapify(skBytes);

      case HLL:
        return HllSketch.heapify(skBytes);

      case CPC:
        return CpcSketch.heapify(skBytes);

      case KLL:
        return KllFloatsSketch.heapify(skBytes);

      case FREQUENCY:
        return ItemsSketch.getInstance(skBytes, new ArrayOfStringsSerDe());

      case RESERVOIR:
        return ReservoirItemsSketch.heapify(skBytes, new ArrayOfStringsSerDe());

      case VAROPT:
        return VarOptItemsSketch.heapify(skBytes, new ArrayOfStringsSerDe());

      default:
        throw new SketchesException("Unsupported sketch family: " + family.toString());
    }
  }

  @SuppressWarnings("unchecked")
  private static byte[] mergeSketches(final Family family, final int k,
                                      final SketchStorage.SketchEntry dstEntry, final ArrayList<Object> sketchList) {
    if (family == null || sketchList.size() == 0) {
      return null;
    }

    switch (family) {
      case UNION:
      case QUICKSELECT: {
        // for HLL, the destination is already a union so no need to add explicitly
        final Union dst = dstEntry == null ? new SetOperationBuilder().setNominalEntries(1 << k).buildUnion()
            : (Union) dstEntry.sketch;
        for (final Object obj : sketchList) {
          dst.update((CompactSketch) obj);
        }

        if (dstEntry == null) {
          return dst.getResult().toByteArray();
        } else {
          dstEntry.sketch = dst;
          return null;
        }
      }

      case HLL: {
        final org.apache.datasketches.hll.Union union = new org.apache.datasketches.hll.Union(k);
        if (dstEntry != null) {
          union.update((HllSketch) dstEntry.sketch);
        }
        for (final Object obj : sketchList) {
          union.update((HllSketch) obj);
        }

        if (dstEntry == null) {
          return union.getResult().toCompactByteArray();
        } else {
          dstEntry.sketch = union.getResult();
          return null;
        }
      }

      case CPC: {
        final CpcUnion union = new CpcUnion(k);
        if (dstEntry != null) {
          union.update((CpcSketch) dstEntry.sketch);
        }
        for (final Object obj : sketchList) {
          union.update((CpcSketch) obj);
        }

        if (dstEntry == null) {
          return union.getResult().toByteArray();
        } else {
          dstEntry.sketch = union.getResult();
          return null;
        }
      }

      case KLL: {
        // Only merge(), no separate union. Slightly abusing terminology to call it union
        final KllFloatsSketch union = dstEntry == null ? new KllFloatsSketch(k) : (KllFloatsSketch) dstEntry.sketch;

        for (final Object obj : sketchList) {
          union.merge((KllFloatsSketch) obj);
        }

        if (dstEntry == null) {
          return union.toByteArray();
        } else {
          dstEntry.sketch = union;
          return null;
        }
      }

      case FREQUENCY: {
        // Only merge(), no separate union. Slightly abusing terminology to call it union
        final ItemsSketch<String> union = dstEntry == null ? new ItemsSketch<>(k) : (ItemsSketch<String>) dstEntry.sketch;

        for (final Object obj : sketchList) {
          union.merge((ItemsSketch<String>) obj);
        }

        if (dstEntry == null) {
          return union.toByteArray(new ArrayOfStringsSerDe());
        } else {
          dstEntry.sketch = union;
          return null;
        }
      }

      case RESERVOIR: {
        final ReservoirItemsUnion<String> union = ReservoirItemsUnion.newInstance(k);
        if (dstEntry != null) {
          union.update((ReservoirItemsSketch<String>) dstEntry.sketch);
        }

        for (final Object obj : sketchList) {
          union.update((ReservoirItemsSketch<String>) obj);
        }

        if (dstEntry == null) {
         return union.getResult().toByteArray(new ArrayOfStringsSerDe());
        } else {
          dstEntry.sketch = union.getResult();
          return null;
        }
      }

      case VAROPT: {
        final VarOptItemsUnion<String> union = VarOptItemsUnion.newInstance(k);
        if (dstEntry != null) {
          union.update((VarOptItemsSketch<String>) dstEntry.sketch);
        }

        for (final Object obj : sketchList) {
          union.update((VarOptItemsSketch<String>) obj);
        }

        if (dstEntry == null) {
          return union.getResult().toByteArray(new ArrayOfStringsSerDe());
        } else {
          dstEntry.sketch = union.getResult();
          return null;
        }
      }

      default:
        throw new SketchesException("Unsupported sketch family: " + family.toString());
    }
  }
}
