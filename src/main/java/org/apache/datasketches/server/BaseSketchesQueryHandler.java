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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.Reader;
import java.net.URLDecoder;

import org.apache.datasketches.Family;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import static org.apache.datasketches.server.SketchConstants.*;

/**
 * Provides a common request handler for the sketches server. This gives us several benefits:
 * <ul>
 *   <li>Extracts JSON query from querystring or POST body, as appropriate, to allow multiple input types.</li>
 *   <li>Sketches are stateful, and even reading can be disrupted by writes on another thread. A real
 *       database would have a more sophisticated locking system, but this class lets us synchronize across
 *       query types.</li>
 *   <li>Handles both JSON arrays or single JSON objects as inputs, letting the query handlers avoid
 *       code duplication.
 * </ul>
 * By using this class, the individual query handlers are able to consume and emit only JSON objects; they
 * need not worry about details of the HTTP request or response.
 */
public abstract class BaseSketchesQueryHandler extends AbstractHandler {
  final SketchStorage sketches;
  final boolean queryExempt;

  /**
   * Basic query handler. Assumes calls must include a JSON query.
   * @param sketches The sketches database to use
   */
  BaseSketchesQueryHandler(final SketchStorage sketches) {
    this(sketches, false);
  }

  /**
   * Basic query handler, allowing the derived type to explicitly declare if an input query is optional.
   * @param sketches The sketches database to use
   * @param queryExempt <tt>true</tt> if a query is not required, otherwise <tt>false</tt>
   */
  BaseSketchesQueryHandler(final SketchStorage sketches, final boolean queryExempt) {
    if (sketches == null) {
      throw new IllegalArgumentException("Cannot initialize handler with SketchStorage == null");
    }
    this.sketches = sketches;
    this.queryExempt = queryExempt;
  }

  static JsonElement checkMethodAndReadJson(final Request baseRequest,
                                            final HttpServletRequest request,
                                            final HttpServletResponse response) throws IOException {
    JsonElement query = null;
    if (request.getMethod().equals("POST")) {
      response.setContentType("application/json");
      try (final Reader reader = request.getReader()) {
        query = JsonParser.parseReader(reader);
      }
    } else if (request.getMethod().equals("GET")) {
      response.setContentType("text/html");
      query = JsonParser.parseString(URLDecoder.decode(request.getQueryString(), "utf-8"));
    } else {
      response.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
      baseRequest.setHandled(true);
    }

    return query;
  }

  /**
   * Query handler to be implemented by subclasses
   * @param query A JSON query to process
   * @return A JSON response
   */
  protected abstract JsonObject processQuery(JsonObject query);

  /**
   * Internal method to synchronize calls to subclasses
   * @param query A JSON query to process
   * @return A JSON response
   */
  final synchronized JsonObject callProcessQuery(final JsonObject query) {
    return processQuery(query);
  }

  @Override
  public void handle(final String target,
                     final Request baseRequest,
                     final HttpServletRequest request,
                     final HttpServletResponse response) throws IOException {
    JsonElement query = null;
    if (!queryExempt && ((query = checkMethodAndReadJson(baseRequest, request, response)) == null)) {
      return;
    }

    // error messages will be wrapped in json
    response.setCharacterEncoding("utf-8");
    response.setContentType("application/json");

    JsonElement result = null;

    try {
      if (query == null) {
        result = callProcessQuery(null);
      } else if (query.isJsonArray()) {
        for (final JsonElement subQuery : query.getAsJsonArray()) {
          final JsonObject subResult = callProcessQuery(subQuery.getAsJsonObject());
          if (subResult != null) {
            // lazy initialization to avoid possibly empty array
            if (result == null) {
              result = new JsonArray(((JsonArray) query).size());
            }
            ((JsonArray) result).add(subResult);
          }
        }
      } else {
        result = callProcessQuery((JsonObject) query);
      }

      if (result != null) {
        //response.getWriter().print(result.toString());
        response.getWriter().print(new GsonBuilder().setPrettyPrinting().create().toJson(result));
      }

      // we're ok if we reach here without an exception
      response.setStatus(HttpServletResponse.SC_OK);
    } catch (final Exception e) {
      final JsonObject error = new JsonObject();
      error.addProperty(ERROR_KEY, e.getMessage());
      response.setStatus(UNPROCESSABLE_ENTITY);
    }

    baseRequest.setHandled(true);
  }

  static Family familyFromString(final String type) throws IllegalArgumentException {
    switch (type.toLowerCase()) {
      case SKETCH_FAMILY_THETA:
        return Family.QUICKSELECT;

      case SKETCH_FAMILY_KLL:
        return Family.KLL;

      case SKETCH_FAMILY_FREQUENCY:
        return Family.FREQUENCY;

      case SKETCH_FAMILY_HLL:
        return Family.HLL;

      case SKETCH_FAMILY_CPC:
        return Family.CPC;

      case SKETCH_FAMILY_RESERVOIR:
        return Family.RESERVOIR;

      case SKETCH_FAMILY_VAROPT:
        return Family.VAROPT;

      default:
        throw new IllegalArgumentException("Unrecognized sketch type: " + type);
    }
  }
}
