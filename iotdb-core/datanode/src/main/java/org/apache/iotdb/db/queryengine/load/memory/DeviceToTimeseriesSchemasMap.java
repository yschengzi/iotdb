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

package org.apache.iotdb.db.queryengine.load.memory;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.load.memory.block.LoadMemoryBlock;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DeviceToTimeseriesSchemasMap {
  private final LoadTsFileMemoryManager loadTsFileMemoryManager =
      LoadTsFileMemoryManager.getInstance();

  private final LoadMemoryBlock block;

  private long totalMemorySizeInBytes =
      IoTDBDescriptor.getInstance().getConfig().getInitLoadMemoryTotalSizeInBytes() / 2;
  private long usedMemorySizeInBytes = 0;

  private final Map<String, Set<MeasurementSchema>> currentBatchDevice2TimeseriesSchemas =
      new HashMap<>();

  // true if the memory is still enough and the schema is added successfully, false when it is out
  // of memory.
  public boolean add(String device, MeasurementSchema measurementSchema) {
    currentBatchDevice2TimeseriesSchemas
        .computeIfAbsent(device, k -> new HashSet<>())
        .add(measurementSchema);

    usedMemorySizeInBytes += measurementSchema.serializedSize();
    return usedMemorySizeInBytes <= totalMemorySizeInBytes;
  }

  public void clear() {
    currentBatchDevice2TimeseriesSchemas.clear();
    usedMemorySizeInBytes = 0;
  }

  public boolean isEmpty() {
    return currentBatchDevice2TimeseriesSchemas.isEmpty();
  }

  public Set<Map.Entry<String, Set<MeasurementSchema>>> entrySet() {
    return currentBatchDevice2TimeseriesSchemas.entrySet();
  }

  public Set<String> keySet() {
    return currentBatchDevice2TimeseriesSchemas.keySet();
  }

  public void distory() {
    currentBatchDevice2TimeseriesSchemas.clear();
    totalMemorySizeInBytes = 0;
    usedMemorySizeInBytes = 0;
    loadTsFileMemoryManager.release(block);
  }

  ///////////////////////////// SINGLETON /////////////////////////////
  private DeviceToTimeseriesSchemasMap() {
    block = loadTsFileMemoryManager.forceAllocate(totalMemorySizeInBytes);
  }

  public static DeviceToTimeseriesSchemasMap getInstance() {
    return LoadTsFileAnalyzerMemoryManagerHolder.INSTANCE;
  }

  public static class LoadTsFileAnalyzerMemoryManagerHolder {
    private static final DeviceToTimeseriesSchemasMap INSTANCE = new DeviceToTimeseriesSchemasMap();
  }
}
