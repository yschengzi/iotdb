/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.collector.realtime;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.collector.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.pipe.config.constant.PipeCollectorConstant;
import org.apache.iotdb.db.pipe.config.plugin.env.PipeTaskCollectorRuntimeEnvironment;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeCollectEvent;
import org.apache.iotdb.pipe.api.PipeCollector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeCollectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

public abstract class PipeRealtimeDataRegionCollector implements PipeCollector {

  private String pattern;
  private TConsensusGroupId dataRegionId;
  private PipeStaticMeta pipeStaticMeta;

  public PipeRealtimeDataRegionCollector() {}

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {}

  @Override
  public void customize(PipeParameters parameters, PipeCollectorRuntimeConfiguration configuration)
      throws Exception {
    pattern =
        parameters.getStringOrDefault(
            PipeCollectorConstant.COLLECTOR_PATTERN_KEY,
            PipeCollectorConstant.COLLECTOR_PATTERN_DEFAULT_VALUE);

    final PipeTaskCollectorRuntimeEnvironment environment =
        (PipeTaskCollectorRuntimeEnvironment) configuration.getRuntimeEnvironment();
    dataRegionId = environment.getRegionId();
    pipeStaticMeta = environment.getPipeStaticMeta();
  }

  @Override
  public void start() throws Exception {
    PipeInsertionDataNodeListener.getInstance()
        .startListenAndAssign(
            String.valueOf(
                ConsensusGroupId.Factory.createFromTConsensusGroupId(dataRegionId).getId()),
            this);
  }

  @Override
  public void close() throws Exception {
    PipeInsertionDataNodeListener.getInstance()
        .stopListenAndAssign(
            String.valueOf(
                ConsensusGroupId.Factory.createFromTConsensusGroupId(dataRegionId).getId()),
            this);
  }

  /** @param event the event from the storage engine */
  public abstract void collect(PipeRealtimeCollectEvent event);

  public abstract boolean isNeedListenToTsFile();

  public abstract boolean isNeedListenToInsertNode();

  public final String getPattern() {
    return pattern;
  }

  public final PipeStaticMeta getPipeStaticMeta() {
    return pipeStaticMeta;
  }

  public final TConsensusGroupId getDataRegionId() {
    return dataRegionId;
  }

  @Override
  public String toString() {
    return "PipeRealtimeDataRegionCollector{"
        + "pattern='"
        + pattern
        + '\''
        + ", dataRegionId='"
        + dataRegionId
        + '\''
        + '}';
  }
}