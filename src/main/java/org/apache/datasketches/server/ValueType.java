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

/**
 * An enum to hold the known data value types.
 */
public enum ValueType {

  /**
   * Character string. Uses default encoding.
   */
  STRING(SketchConstants.VALUE_TYPE_STRING),

  /**
   * 32-bit floating point value
   */
  FLOAT(SketchConstants.VALUE_TYPE_FLOAT),

  /**
   * 64-bit floating point value
   */
  DOUBLE(SketchConstants.VALUE_TYPE_DOUBLE),

  /**
   * 32-bit signed integer value
   */
  INT(SketchConstants.VALUE_TYPE_INT),

  /**
   * 64-bit signed integer value
   */
  LONG(SketchConstants.VALUE_TYPE_LONG);


  private static final HashMap<String, ValueType> lookupTypeName = new HashMap<>();
  private final String typeName_;

  static {
    for (ValueType t : values()) {
      lookupTypeName.put(t.getTypeName().toLowerCase(), t);
    }
  }

  ValueType(final String typeName) {
    typeName_ = typeName;
  }

  public String getTypeName() {
    return typeName_;
  }

  /**
   * Returns the ValueType given the type name
   * @param typeName the family name
   * @return the ValueType given the type name
   */
  public static ValueType stringToType(final String typeName) {
    final ValueType t = lookupTypeName.get(typeName.toLowerCase());
    if (t == null) {
      throw new IllegalArgumentException("Illegal ValueType Name: " + typeName);
    }
    return t;
  }


}
