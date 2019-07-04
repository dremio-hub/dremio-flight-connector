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

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import org.apache.arrow.vector.types.pojo.ArrowType.Date;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Interval;
import org.apache.arrow.vector.types.pojo.ArrowType.List;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.ArrowType.Time;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;

/***
 * Gets Dremio RPC-/protobuf-level data type for given SQL data type name.
 * returns the canonical keyword sequence for SQL data type (leading keywords in
 * corresponding {@code <data type>}; what
 * {@code INFORMATION_SCHEMA.COLUMNS.TYPE_NAME} would list)
 */
public class SqlTypeNameToArrowType {

  public static ArrowType toArrowType(String typeName) {
    switch (typeName) {
      case "NULL":
        return new Null();
      case "MAP":
        return new Struct(); //todo inner type?
      case "ARRAY":
        return new List(); //todo inner type?
      case "UNION":
        throw new UnsupportedOperationException("have not implemented unions");
        //return new Union(); //todo inner type?
      case "TINYINT":
        return new Int(8, true);
      case "SMALLINT":
        return new Int(16, true);
      case "INTEGER":
        return new Int(32, true);
      case "BIGINT":
        return new Int(64, true);
      case "FLOAT":
        return new FloatingPoint(FloatingPointPrecision.SINGLE);
      case "DOUBLE":
        return new FloatingPoint(FloatingPointPrecision.DOUBLE);
      case "CHARACTER VARYING":
        return new Utf8();
      case "BINARY VARYING":
        return new Binary();
      case "BOOLEAN":
        return new Bool();
      case "DECIMAL":
        throw new UnsupportedOperationException("have not implemented decimal");
      case "DATE":
        return new Date(DateUnit.MILLISECOND);
      case "TIME":
        return new Time(TimeUnit.MICROSECOND, 64);
      case "TIMESTAMP":
        return new Timestamp(TimeUnit.MICROSECOND, "UTC");
      case "INTERVAL DAY TO SECOND":
        return new Interval(IntervalUnit.DAY_TIME);
      case "INTERVAL YEAR TO MONTH":
        return new Interval(IntervalUnit.YEAR_MONTH);
      case "BINARY":
        throw new UnsupportedOperationException("have not implemented binary fixed size");
      default:
        throw new IllegalStateException("unable to find arrow type for " + typeName);
    }
  }
}
