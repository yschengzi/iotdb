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

package org.apache.iotdb.commons.consensus.index;

import com.google.common.collect.ImmutableList;
import org.apache.iotdb.commons.consensus.index.impl.HybridProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public interface ProgressIndex {

  /** Serialize this progress index to the given byte buffer. */
  void serialize(ByteBuffer byteBuffer);

  /** Serialize this progress index to the given output stream. */
  void serialize(OutputStream stream) throws IOException;

  /**
   * A.isAfter(B) is true if and only if A is strictly greater than B.
   *
   * @param progressIndex the progress index to be compared
   * @return true if and only if this progress index is strictly greater than the given consensus
   *     index
   */
  boolean isAfter(@Nonnull ProgressIndex progressIndex);

  /**
   * A.equals(B) is true if and only if A is equal to B
   *
   * @param progressIndex the progress index to be compared
   * @return true if and only if this progress index is equal to the given progress index
   */
  boolean equals(ProgressIndex progressIndex);

  /**
   * A.equals(B) is true if and only if A is equal to B
   *
   * @param obj the object to be compared
   * @return true if and only if this progress index is equal to the given object
   */
  @Override
  boolean equals(Object obj);

  /**
   * C = A.updateToMinimumIsAfterProgressIndex(B) where C should satisfy:
   *
   * <p>(C.equals(A) || C.isAfter(A)) is true
   *
   * <p>(C.equals(B) || C.isAfter(B)) is true
   *
   * <p>There is no D, such that D satisfies the above conditions and C.isAfter(D) is true
   *
   * <p>The implementation of this function should be reflexive, that is
   * A.updateToMinimumIsAfterProgressIndex(B).equals(B.updateToMinimumIsAfterProgressIndex(A)) is
   * true
   *
   * <p>Note: this function may modify the caller.
   *
   * @param progressIndex the progress index to be compared
   * @return the minimum progress index after the given progress index and this progress index
   */
  ProgressIndex updateToMinimumIsAfterProgressIndex(ProgressIndex progressIndex);

  /**
   * @return the type of this progress index
   */
  ProgressIndexType getType();

  TotalOrderSumTuple getTotalOrderSumTuple();

  /**
   * Blend two progress index together, the result progress index should satisfy:
   *
   * <p>(result.equals(progressIndex1) || result.isAfter(progressIndex1)) is true
   *
   * <p>(result.equals(progressIndex2) || result.isAfter(progressIndex2)) is true
   *
   * <p>There is no R, such that R satisfies the above conditions and result.isAfter(R) is true
   *
   * @param progressIndex1 the first progress index. if it is null, the result progress index should
   *     be the second progress index. if it is a minimum progress index, the result progress index
   *     should be the second progress index. (if the second progress index is null, the result
   *     should be a minimum progress index). if it is a hybrid progress index, the result progress
   *     index should be the minimum progress index after the second progress index and the first
   *     progress index
   * @param progressIndex2 the second progress index. if it is null, the result progress index
   *     should be the first progress index. if it is a minimum progress index, the result progress
   *     index should be the first progress index. (if the first progress index is null, the result
   *     should be a minimum progress index). if it is a hybrid progress index, the result progress
   *     index should be the minimum progress index after the first progress index and the second
   *     progress index
   * @return the minimum progress index after the first progress index and the second progress index
   */
  static ProgressIndex blendProgressIndex(
      ProgressIndex progressIndex1, ProgressIndex progressIndex2) {
    if (progressIndex1 == null && progressIndex2 == null) {
      return MinimumProgressIndex.INSTANCE;
    }
    if (progressIndex1 == null || progressIndex1 instanceof MinimumProgressIndex) {
      return progressIndex2 == null ? MinimumProgressIndex.INSTANCE : progressIndex2;
    }
    if (progressIndex2 == null || progressIndex2 instanceof MinimumProgressIndex) {
      return progressIndex1; // progressIndex1 is not null
    }

    return new HybridProgressIndex(progressIndex1.getType().getType(), progressIndex1)
        .updateToMinimumIsAfterProgressIndex(progressIndex2);
  }

  static void topologicalSort(List<ProgressIndex> progressIndexList) {
    progressIndexList.sort(Comparator.comparing(ProgressIndex::getTotalOrderSumTuple));
  }

  class TotalOrderSumTuple implements Comparable<TotalOrderSumTuple> {
    private final ImmutableList<Long> tuple;

    public TotalOrderSumTuple(Long... args) {
      tuple = ImmutableList.copyOf(args);
    }

    public TotalOrderSumTuple(List<Long> list) {
      tuple = ImmutableList.copyOf(list);
    }

    @Override
    public int compareTo(TotalOrderSumTuple that) {
      if (that.tuple.size() != this.tuple.size()) {
        return this.tuple.size() - that.tuple.size();
      }
      for (int i = this.tuple.size() - 1; i >= 0; --i) {
        if (!this.tuple.get(i).equals(that.tuple.get(i))) {
          return this.tuple.get(i) < that.tuple.get(i) ? -1 : 1;
        }
      }
      return 0;
    }

    public static TotalOrderSumTuple sum(List<TotalOrderSumTuple> tupleList) {
      if (tupleList == null || tupleList.size() == 0) {
        return new TotalOrderSumTuple();
      }
      if (tupleList.size() == 1) {
        return tupleList.get(0);
      }

      List<Long> result =
          LongStream.range(0, tupleList.stream().mapToInt(t -> t.tuple.size()).max().getAsInt())
              .map(o -> 0)
              .boxed()
              .collect(Collectors.toList());
      tupleList.forEach(
          t ->
              IntStream.range(0, t.tuple.size())
                  .forEach(i -> result.set(i, result.get(i) + t.tuple.get(i))));
      return new TotalOrderSumTuple(result);
    }
  }
}
