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

package org.apache.iotdb.commons.consensus.index.impl;

import org.apache.iotdb.commons.consensus.index.ConsensusIndex;
import org.apache.iotdb.commons.consensus.index.ConsensusIndexType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class IoTConsensusIndex implements ConsensusIndex {
  private final Map<Integer, Long> peerId2SearchIndex;

  public IoTConsensusIndex() {
    peerId2SearchIndex = new HashMap<>();
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    ConsensusIndexType.IOT_CONSENSUS_INDEX.serialize(byteBuffer);
    // TODO: impl it
  }

  @Override
  public void serialize(OutputStream stream) throws IOException {
    ConsensusIndexType.IOT_CONSENSUS_INDEX.serialize(stream);
    // TODO: impl it
  }

  @Override
  public boolean isAfter(ConsensusIndex consensusIndex) {
    if (!(consensusIndex instanceof IoTConsensusIndex)) {
      return false;
    }

    return ((IoTConsensusIndex) consensusIndex)
        .peerId2SearchIndex.entrySet().stream()
            .noneMatch(
                entry ->
                    !this.peerId2SearchIndex.containsKey(entry.getKey())
                        || this.peerId2SearchIndex.get(entry.getKey()) <= entry.getValue());
  }

  @Override
  public ConsensusIndex updateToMaximum(ConsensusIndex consensusIndex) {
    if (!(consensusIndex instanceof IoTConsensusIndex)) {
      return this;
    }

    ((IoTConsensusIndex) consensusIndex)
        .peerId2SearchIndex.forEach(
            (thatK, thatV) ->
                this.peerId2SearchIndex.compute(
                    thatK, (thisK, thisV) -> (thisV == null ? thatV : Math.max(thisV, thatV))));
    return this;
  }

  public static IoTConsensusIndex deserializeFrom(ByteBuffer byteBuffer) {
    // TODO: impl it
    return new IoTConsensusIndex();
  }

  public static IoTConsensusIndex deserializeFrom(InputStream stream) {
    // TODO: impl it
    return new IoTConsensusIndex();
  }
}