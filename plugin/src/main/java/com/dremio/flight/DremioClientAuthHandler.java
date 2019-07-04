/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.flight;

import java.util.Iterator;

import org.apache.arrow.flight.auth.ClientAuthHandler;
import org.glassfish.jersey.internal.util.Base64;

/**
 * Basic auth client handler to make compatible with Python
 */
public class DremioClientAuthHandler implements ClientAuthHandler {
  private final String name;
  private final String password;
  private byte[] token = null;

  public DremioClientAuthHandler(String name, String password) {
    this.name = name;
    this.password = password;
  }

  public void authenticate(ClientAuthSender outgoing, Iterator<byte[]> incoming) {
    StringBuffer builder = new StringBuffer();
    if (this.name != null) {
      builder.append(this.name);
    }
    builder.append(":");
    if (this.password != null) {
      builder.append(this.password);
    }

    outgoing.send(Base64.encode(builder.toString().getBytes()));
    this.token = (byte[]) incoming.next();
  }

  public byte[] getCallToken() {
    return this.token;
  }
}
