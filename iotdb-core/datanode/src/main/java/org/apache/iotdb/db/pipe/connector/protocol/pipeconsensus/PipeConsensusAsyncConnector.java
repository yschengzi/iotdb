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

package org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.IoTDBDataRegionAsyncConnector;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class PipeConsensusAsyncConnector extends IoTDBDataRegionAsyncConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConsensusAsyncConnector.class);

  private static final String ENQUEUE_EXCEPTION_MSG =
      "Timeout: PipeConsensusConnector offers an event into transferBuffer failed, because transferBuffer is full";

  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  private final BlockingQueue<Event> transferBuffer =
      new LinkedBlockingDeque<>(COMMON_CONFIG.getPipeConsensusEventBufferSize());

  /** Add an event to transferBuffer, whose events will be asynchronizedly transfer to receiver. */
  private boolean addEvent2Buffer(Event event) {
    try {
      LOGGER.debug(
          "PipeConsensus connector: one event enqueue, queue size = {}, limit size = {}",
          transferBuffer.size(),
          COMMON_CONFIG.getPipeConsensusEventBufferSize());
      return transferBuffer.offer(
          event, COMMON_CONFIG.getPipeConsensusEventEnqueueTimeoutInMs(), TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOGGER.info("PipeConsensusConnector transferBuffer queue offer is interrupted.", e);
      Thread.currentThread().interrupt();
      return false;
    }
  }

  /**
   * if one event is successfully processed by receiver in PipeConsensus, we will remove this event
   * from transferBuffer in order to transfer other event.
   */
  public void removeEventFromBuffer(Event event) {
    synchronized (this) {
      LOGGER.debug(
          "PipeConsensus connector: one event removed from queue, queue size = {}, limit size = {}",
          transferBuffer.size(),
          COMMON_CONFIG.getPipeConsensusEventBufferSize());
      Iterator<Event> iterator = transferBuffer.iterator();
      Event current = iterator.next();
      while (!current.equals(event) && iterator.hasNext()) {
        current = iterator.next();
      }
      iterator.remove();
    }
  }

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
    boolean enqueueResult = addEvent2Buffer(tabletInsertionEvent);
    if (!enqueueResult) {
      throw new PipeException(ENQUEUE_EXCEPTION_MSG);
    }
    // TODO：改造 request，加上 commitId 和 rebootTimes
    // TODO: 改造 handler，onComplete 的优化 + onComplete 加上出队逻辑
    super.transfer(tabletInsertionEvent);
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    boolean enqueueResult = addEvent2Buffer(tsFileInsertionEvent);
    if (!enqueueResult) {
      throw new PipeException(ENQUEUE_EXCEPTION_MSG);
    }
    super.transfer(tsFileInsertionEvent);
  }

  @Override
  public void transfer(Event event) throws Exception {
    boolean enqueueResult = addEvent2Buffer(event);
    if (!enqueueResult) {
      throw new PipeException(ENQUEUE_EXCEPTION_MSG);
    }
    super.transfer(event);
  }
}
