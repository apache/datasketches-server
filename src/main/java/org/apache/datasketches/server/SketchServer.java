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

import java.io.IOException;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.ErrorHandler;

/**
 * Creates a very basic sketch server running embedded Jetty. Configuration options are specified in a config
 * file; for details @see SketchServerConfig.
 */
public class SketchServer {
  private final SketchServerConfig config;
  private SketchStorage sketches;
  private Server server;

  /**
   * Creates a server with the provided configuration
   * @param configFile Path to a configuration file following the <tt>SketchServerConfig</tt> format
   * @throws IOException
   */
  public SketchServer(final String configFile) throws IOException {
    config = new SketchServerConfig(configFile);
  }

  // defines paths and registers the relevant handlers
  private void createServer() {
    server = new Server(config.getPort());

    // Error page unless you have a correct URL
    ContextHandler contextRoot = new ContextHandler("/");
    contextRoot.setContextPath("/");
    contextRoot.setHandler(new ErrorHandler());

    ContextHandler contextStatus = new ContextHandler(SketchConstants.STATUS_PATH);
    contextStatus.setHandler(new StatusHandler(sketches));

    ContextHandler contextSerialize = new ContextHandler(SketchConstants.SERIALIZE_PATH);
    contextSerialize.setHandler(new SerializationHandler(sketches));

    ContextHandler contextUpdate = new ContextHandler(SketchConstants.UPDATE_PATH);
    contextUpdate.setHandler(new UpdateHandler(sketches));

    ContextHandler contextMerge = new ContextHandler(SketchConstants.MERGE_PATH);
    contextMerge.setHandler(new MergeHandler(sketches));

    ContextHandler contextQuery = new ContextHandler(SketchConstants.QUERY_PATH);
    contextQuery.setHandler(new DataQueryHandler(sketches));

    ContextHandlerCollection contexts =
        new ContextHandlerCollection(contextRoot,
            contextStatus,
            contextSerialize,
            contextUpdate,
            contextMerge,
            contextQuery);
    server.setHandler(contexts);
  }

  /**
   * Initializes the sketches, configures query handlers, and Starts the server.
   * @throws Exception
   */
  public void start() throws Exception {
    sketches = new SketchStorage(config.getSketchList());
    createServer();
    server.start();
  }

  /**
   * Returns the server's base URI
   * @return Server URI as a string
   */
  public String getURI() {
    if (server == null)
      return null;
    return server.getURI().toString();
  }

  /**
   * Returns the server's configured port
   * @return The server's pert
   */
  public int getPort() {
    if (server != null) {
      return ((ServerConnector) server.getConnectors()[0]).getLocalPort();
    }
    return -1;
  }

  public static void main(String[] args) throws Exception {
    //String confFile = args[0];
    String confFile = "conf3.json";

    SketchServer sketchServer = new SketchServer(confFile);
    sketchServer.start();
  }
}
