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
package com.dremio.flight.formation;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.dremio.sabot.op.writer.WriterOperator;

public class FormationWriterCreator implements SingleInputOperator.Creator<FormationWriter> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FormationWriterCreator.class);

  @Override
  public SingleInputOperator create(OperatorContext context, FormationWriter operator) throws ExecutionSetupException {

    FormationRecordWriter writer = new FormationRecordWriter(operator.getLocation(), operator.getPlugin().getProducer(), context.getFragmentHandle());
    return new WriterOperator(context, operator.getOptions(), writer);
  }
}
