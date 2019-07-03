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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.inject.Provider;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.auth.BasicServerAuthHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SessionOptionManager;
import com.dremio.exec.server.options.SessionOptionManagerFactoryImpl;
import com.dremio.sabot.rpc.user.UserRpcUtils;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.users.SystemUser;
import com.dremio.service.users.UserLoginException;
import com.dremio.service.users.UserService;

/**
 * user/pass validation for dremios arrow flight endpoint
 */
public class AuthValidator implements BasicServerAuthHandler.BasicAuthValidator {
  private static final Logger logger = LoggerFactory.getLogger(AuthValidator.class);
  private final Map<ByteArrayWrapper, UserSession> sessions = new HashMap<>();
  private final Map<ByteArrayWrapper, String> passwords = new HashMap<>();
  private final Map<ByteArrayWrapper, FlightSessionOptions> options = new HashMap<>();
  private final Map<String, ByteArrayWrapper> tokens = new HashMap<>();
  private final UserService userService;
  private final SabotContext context;

  public AuthValidator(Provider<UserService> userService, Provider<SabotContext> context) {
    this.userService = userService.get();
    this.context = context.get();
  }

  public AuthValidator(UserService userService, SabotContext context) {
    this.userService = userService;
    this.context = context;
  }


  @Override
  public byte[] getToken(String user, String password) throws Exception {
    try {
      if (userService != null) {
        userService.authenticate(user, password);
      } else {
        if (!(SystemUser.SYSTEM_USERNAME.equals(user) && "".equals(password))) {
          throw new UserLoginException(user, "not default user");
        }
      }
      byte[] b = (user + ":" + password).getBytes();
      sessions.put(new ByteArrayWrapper(b), build(user, password));
      passwords.put(new ByteArrayWrapper(b), password);
      options.put(new ByteArrayWrapper(b), new FlightSessionOptions());
      tokens.put(user, new ByteArrayWrapper(b));
      logger.info("authenticated {}", user);
      return b;
    } catch (Throwable e) {
      logger.error("unable to authenticate {}", user);
    }
    return new byte[0];
  }

  @Override
  public Optional<String> isValid(byte[] bytes) {
    logger.warn("Sessions: " + sessions.keySet().size() + " with entries " + sessions.keySet());
    logger.warn("asking for " + new ByteArrayWrapper(bytes) + " it is " + ((sessions.containsKey(new ByteArrayWrapper(bytes))) ? "in" : "not in") + " the session set");
    UserSession session = sessions.get(new ByteArrayWrapper(bytes));
    String user = null;
    if (session != null) {
      user = session.getCredentials().getUserName();
    }
    return Optional.ofNullable(user);
  }

  private UserSession build(String user, String password) {
    SessionOptionManager optionsManager =
      new SessionOptionManagerFactoryImpl().getOrCreate("flight-session-" + user, context.getOptionManager());
    return UserSession.Builder.newBuilder()
      .withCredentials(UserBitShared.UserCredentials.newBuilder().setUserName(user).build())
      .withSessionOptionManager(optionsManager)
      .withUserProperties(
        UserProtos.UserProperties.newBuilder().addProperties(
          UserProtos.Property.newBuilder().setKey("password").setValue(password).build()
        ).build())
      .withClientInfos(UserRpcUtils.getRpcEndpointInfos("Dremio Flight Client"))
      .setSupportComplexTypes(true)
      .build();
  }

  public UserSession getUserSession(FlightProducer.CallContext callContext) {
    return sessions.get(tokens.get(callContext.peerIdentity()));
  }

  public String getUserPassword(FlightProducer.CallContext callContext) {
    return passwords.get(tokens.get(callContext.peerIdentity()));
  }

  public FlightSessionOptions getSessionOptions(FlightProducer.CallContext callContext) {
    return options.get(tokens.get(callContext.peerIdentity()));
  }

  public static class FlightSessionOptions {
    private boolean isParallel;

    public boolean isParallel() {
      return isParallel;
    }

    public void setParallel(boolean parallel) {
      isParallel = parallel;
    }
  }

  /**
   * wrapper class to make byte[] a map key
   */
  private static class ByteArrayWrapper {
    private final byte[] bytes;

    public ByteArrayWrapper(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ByteArrayWrapper that = (ByteArrayWrapper) o;
      return Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(bytes);
    }
  }
}
