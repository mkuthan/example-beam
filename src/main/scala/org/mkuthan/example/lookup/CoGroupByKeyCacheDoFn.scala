// Copyright (C) 2011-2012 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.mkuthan.example.lookup

import com.spotify.scio.ScioMetrics
import com.spotify.scio.coders.Coder
import com.spotify.scio.coders.CoderMaterializer
import org.apache.beam.sdk.metrics.Counter
import org.apache.beam.sdk.state.StateSpecs
import org.apache.beam.sdk.state.TimeDomain
import org.apache.beam.sdk.state.Timer
import org.apache.beam.sdk.state.TimerSpecs
import org.apache.beam.sdk.state.ValueState
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.OnTimer
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.DoFn.StateId
import org.apache.beam.sdk.transforms.DoFn.TimerId
import org.apache.beam.sdk.values.KV
import org.joda.time.Duration
import org.mkuthan.example.lookup.CoGroupByKeyCacheDoFn.CoGroupByKeyCacheDoFnType

object CoGroupByKeyCacheDoFn {

  type CoGroupByKeyCacheDoFnType[K, V, W] = DoFn[KV[K, (Iterable[V], Iterable[W])],
    KV[K, Iterable[(V, Option[W])]]]

  final val CacheKey = "cache"
  final val ExpiryKey = "expiry"
}

class CoGroupByKeyCacheDoFn[K, V, W](allowedLateness: Duration)
  (implicit c: Coder[W]) extends CoGroupByKeyCacheDoFnType[K, V, W] {

  import CoGroupByKeyCacheDoFn._

  @StateId(CacheKey) private val cacheSpecs = StateSpecs.value[W](
    CoderMaterializer.beamWithDefault(Coder[W]))

  @TimerId(ExpiryKey) private val expirySpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME)

  private val cachedCounter: Counter = ScioMetrics.counter("cached")

  private val expiredCounter: Counter = ScioMetrics.counter("expired")

  @ProcessElement
  def processElement(
      context: CoGroupByKeyCacheDoFnType[K, V, W]#ProcessContext,
      @StateId(CacheKey) cacheState: ValueState[W],
      @TimerId(ExpiryKey) expiryTimer: Timer
  ): Unit = {
    val key = context.element().getKey
    val (values, sideInputs) = context.element().getValue

    cacheSideInputs(sideInputs, cacheState, expiryTimer)
    outputWithSideInput(key, values, context, cacheState)
  }

  @OnTimer(ExpiryKey)
  def onExpiry(@StateId(CacheKey) cacheState: ValueState[W]): Unit = {
    cacheState.clear()
    expiredCounter.inc()
  }

  private def cacheSideInputs(
      sideInputs: Iterable[W],
      cacheState: ValueState[W],
      expiryTimer: Timer
  ) =
    sideInputs.lastOption.foreach { sideInput =>
      cacheState.write(sideInput)
      expiryTimer.offset(allowedLateness).setRelative()
      cachedCounter.inc()
    }

  private def outputWithSideInput(
      key: K,
      values: Iterable[V],
      context: CoGroupByKeyCacheDoFnType[K, V, W]#ProcessContext,
      cacheState: ValueState[W]
  ) = {
    if (!values.isEmpty) {
      val sideInput = Option(cacheState.read())

      val valuesWithSideInput = values.map { v =>
        (v, sideInput)
      }

      context.output(KV.of(key, valuesWithSideInput))
    }
  }
}

