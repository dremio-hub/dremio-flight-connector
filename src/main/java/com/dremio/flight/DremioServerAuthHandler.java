/*
 * Copyright (C) 2017-2019 Dremio Corporation
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

import java.util.Base64;
import java.util.Iterator;
import java.util.Optional;

import org.apache.arrow.flight.auth.ServerAuthHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic auth client handler to make compatible with Python
 */
public class DremioServerAuthHandler implements ServerAuthHandler {
  private static final Logger logger = LoggerFactory.getLogger(DremioServerAuthHandler.class);
  private final DremioAuthValidator authValidator;

  public DremioServerAuthHandler(DremioAuthValidator authValidator) {
    this.authValidator = authValidator;
  }

  public boolean authenticate(ServerAuthSender outgoing, Iterator<byte[]> incoming) {
    byte[] bytes = (byte[]) incoming.next();
    String userPass = new String(Base64.getDecoder().decode(bytes));
    String[] splitUserPass = userPass.split(":");
    String user = "";
    String password = "";
    try {
      if (splitUserPass.length >= 1) {
        user = splitUserPass[0];
      }
      if (splitUserPass.length == 2) {
        password = splitUserPass[1];
      }
      if (splitUserPass.length > 2) {
        logger.debug("Failure parsing auth message");
        return false;
      }
      byte[] token = this.authValidator.getToken(user, password);
      outgoing.send(token);
      return true;
    } catch (Exception ex) {
      logger.debug("Failure parsing auth message.", ex);
    }
    return false;
  }

  public Optional<String> isValid(byte[] token) {
    return this.authValidator.isValid(token);
  }

  /**
   * implementation for user auth checking
   */
  public interface DremioAuthValidator {
    byte[] getToken(String var1, String var2) throws Exception;

    Optional<String> isValid(byte[] var1);
  }
}
