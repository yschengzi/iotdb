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

package org.apache.iotdb.confignode.manager.pipe.runtime;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.load.subscriber.IClusterStatusSubscriber;
import org.apache.iotdb.confignode.manager.load.subscriber.RouteChangeEvent;
import org.apache.iotdb.confignode.manager.load.subscriber.StatisticsChangeEvent;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

public class PipeRuntimeCoordinator implements IClusterStatusSubscriber {

  // shared thread pool in the runtime package
  private static final AtomicReference<ExecutorService> procedureSubmitterHolder =
      new AtomicReference<>();
  private final ExecutorService procedureSubmitter;
  private static final boolean enablePipeHeartbeat =
      PipeConfig.getInstance().isEnablePipeHeartbeat();

  private final PipeLeaderChangeHandler pipeLeaderChangeHandler;
  private final PipeHeartbeatParser pipeHeartbeatParser;
  private final PipeMetaSyncer pipeMetaSyncer;
  private final PipeHeartbeatScheduler pipeHeartbeatScheduler;

  public PipeRuntimeCoordinator(ConfigManager configManager) {
    if (procedureSubmitterHolder.get() == null) {
      synchronized (PipeRuntimeCoordinator.class) {
        if (procedureSubmitterHolder.get() == null) {
          procedureSubmitterHolder.set(
              IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
                  ThreadName.PIPE_RUNTIME_PROCEDURE_SUBMITTER.getName()));
        }
      }
    }
    procedureSubmitter = procedureSubmitterHolder.get();

    pipeLeaderChangeHandler = new PipeLeaderChangeHandler(configManager);
    pipeHeartbeatParser = new PipeHeartbeatParser(configManager);
    pipeMetaSyncer = new PipeMetaSyncer(configManager);
    pipeHeartbeatScheduler = new PipeHeartbeatScheduler(configManager);
  }

  public ExecutorService getProcedureSubmitter() {
    return procedureSubmitter;
  }

  @Override
  public void onClusterStatisticsChanged(StatisticsChangeEvent event) {
    pipeLeaderChangeHandler.onClusterStatisticsChanged(event);
  }

  @Override
  public void onRegionGroupLeaderChanged(RouteChangeEvent event) {
    pipeLeaderChangeHandler.onRegionGroupLeaderChanged(event);
  }

  public void parseHeartbeat(
      int dataNodeId, @NotNull List<ByteBuffer> pipeMetaByteBufferListFromDataNode) {
    pipeHeartbeatParser.parseHeartbeat(dataNodeId, pipeMetaByteBufferListFromDataNode);
  }

  public void startPipeMetaSync() {
    pipeMetaSyncer.start();
  }

  public void stopPipeMetaSync() {
    pipeMetaSyncer.stop();
  }

  public void startPipeHeartbeat() {
    if (enablePipeHeartbeat) {
      pipeHeartbeatScheduler.start();
    }
  }

  public void stopPipeHeartbeat() {
    if (enablePipeHeartbeat) {
      pipeHeartbeatScheduler.stop();
    }
  }
}
