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
import java.util.Map;
import java.util.Optional;

import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.service.users.SystemUser;
import com.dremio.service.users.UserLoginException;
import com.dremio.service.users.UserService;
import com.google.common.collect.Maps;

/**
 * user/pass validation for dremios arrow flight endpoint
 */
public class AuthValidator implements DremioServerAuthHandler.DremioAuthValidator {
  private static final Logger logger = LoggerFactory.getLogger(AuthValidator.class);
  private final Map<String, String> passwd = Maps.newHashMap();
  private final Map<ByteArrayWrapper, String> users = Maps.newHashMap();
  private final Provider<UserService> userService;

  public AuthValidator(Provider<UserService> userService) {
    this.userService = userService;
  }

  @Override
  public byte[] getToken(String user, String password) throws Exception {
    UserService userService = this.userService.get();
    try {
      if (userService != null) {
        userService.authenticate(user, password);
      } else {
        if (!(SystemUser.SYSTEM_USERNAME.equals(user) && "".equals(password))) {
          throw new UserLoginException(user, "not default user");
        }
      }
      byte[] b = (user + ":" + password).getBytes();
      users.put(new ByteArrayWrapper(b), user);
      passwd.put(user, password);
      logger.info("authenticated {}", user);
      return b;
    } catch (Throwable e) {
      logger.error("unable to authenticate {}", user);
    }
    return new byte[0];
  }

  @Override
  public Optional<String> isValid(byte[] bytes) {
    String user = users.get(new ByteArrayWrapper(bytes));
    return Optional.ofNullable(user);
  }

  Optional<String> password(String user) {
    return Optional.ofNullable(passwd.get(user));
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
