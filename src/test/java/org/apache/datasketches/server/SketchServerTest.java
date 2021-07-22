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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.util.Objects;

import org.testng.annotations.Test;

public class SketchServerTest {
  @Test
  public void createServer() {
    final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    SketchServer server = null;
    try {
      server = new SketchServer(Objects.requireNonNull(classLoader.getResource("test_config.json")).getFile());
    } catch (final IOException e) {
      fail();
    }

    // check that port and URI are invalid before starting the server
    assertNotNull(server);
    assertFalse(server.isRunning());
    assertNull(server.getURI());
    assertEquals(server.getPort(), -1);
    try {
      server.start();
      assertTrue(server.isRunning());

      // add the few tests in the try block for code simplicity
      assertEquals(server.getPort(), 8080);
      // initial testing suggests it's just using the host's IP address so just checking that the port
      // is working correctly
      assertTrue(server.getURI().endsWith(":" + server.getPort() + "/"));

      server.stop();
      assertFalse(server.isRunning());
    } catch (final Exception e) {
      fail();
    }
  }
}
