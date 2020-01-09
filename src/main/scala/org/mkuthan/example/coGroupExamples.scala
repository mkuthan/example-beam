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

package org.mkuthan.example

import com.spotify.scio._
import com.spotify.scio.coders.Coder
import com.spotify.scio.coders.CoderMaterializer
import com.spotify.scio.values.SCollection
import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.io.GenerateSequence
import org.apache.beam.sdk.metrics.Counter
import org.apache.beam.sdk.options.StreamingOptions
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
import org.apache.beam.sdk.transforms.windowing.AfterPane
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration

/**
 * Slowly-changing lookup cache from unbounded source using CoGroupByKey strategy.
 * Unbounded lookup cache consists of incremental changes,
 * the final lookup cache state has to be built from observed changes.
 *
 * Please refer to:
 * https://cloud.google.com/blog/products/gcp/guide-to-common-cloud-dataflow-use-case-patterns-part-1
 *
 */
object CoGroupExamples {

  val InitialLookupValues = Seq(0L -> "A", 1L -> "B", 2L -> "C", 3L -> "D", 4L -> "E")

  val LookupAccumulationMode = AccumulationMode.DISCARDING_FIRED_PANES

  type Key = Long
  type Value = String
  type Lookup = String

  /**
   * Initial lookup data (e.g loaded once from BigQuery) used when streaming job is started.
   */
  def loadInitialLookup()
    (implicit sc: ScioContext): SCollection[(Key, Lookup)] = {
    sc.parallelize(InitialLookupValues)
      .withGlobalWindow(options = WindowOptions(
        trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(1)),
        accumulationMode = LookupAccumulationMode
      ))
  }

  /**
   * Lookup data incremental changes (e.g. published on PubSub).
   */
  def generateLookupStream()
    (implicit sc: ScioContext): SCollection[(Key, Lookup)] =
    sc.customInput(
      "generateLookupStream",
      GenerateSequence
        .from(0)
        .withRate(1, Duration.standardSeconds(5))
    ).withGlobalWindow(
      options = WindowOptions(
        trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(1)),
        accumulationMode = LookupAccumulationMode
      )
    ).map { i =>
      2L -> s"X$i"
    }

  /**
   * Main stream of data (e.g published on PubSub)
   */
  def generateMainStream()
    (implicit sc: ScioContext): SCollection[(Key, Value)] =
    sc.customInput(
      "generateMainStream",
      GenerateSequence
        .from(0)
        .withRate(1, Duration.standardSeconds(1))
    ).withGlobalWindow(
      options = WindowOptions(
        trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(1)),
        accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES
      )
    ).map { i =>
      val key = i % 5
      key -> s"$i"
    }
}

object CoGroupExample {

  import CoGroupExamples._

  def main(cmdlineArgs: Array[String]): Unit = {
    implicit val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.optionsAs[StreamingOptions].setStreaming(true)

    val initialLookup = loadInitialLookup()

    val lookupStream = generateLookupStream()
    val finalLookupStream = initialLookup.union(lookupStream)

    val mainStream = generateMainStream()

    val finalStream = mainStream
      .cogroup(finalLookupStream)
      .map { case (key, value) => key -> value }
      .applyPerKeyDoFn(new SideInputCacheDoFn(Duration.standardMinutes(10)))

    finalStream.debug()

    sc.run()
  }

  type SideInputCacheDoFnType[K, V, SideInput] = DoFn[KV[K, (Iterable[V], Iterable[SideInput])],
    KV[K, Iterable[(V, Option[SideInput])]]]

  object SideInputCacheDoFn {
    final val CacheKey = "cache"
    final val ExpiryKey = "expiry"
  }

  class SideInputCacheDoFn[K, V, SideInput](allowedLateness: Duration)
    (implicit c: Coder[SideInput]) extends SideInputCacheDoFnType[K, V, SideInput] {

    import SideInputCacheDoFn._

    @StateId(CacheKey) private val cacheSpecs = StateSpecs.value[SideInput](
      CoderMaterializer.beamWithDefault(Coder[SideInput]))

    @TimerId(ExpiryKey) private val expirySpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME)

    private val cachedCounter: Counter = ScioMetrics.counter("cached")

    private val expiredCounter: Counter = ScioMetrics.counter("expired")

    @ProcessElement
    def processElement(
        context: SideInputCacheDoFnType[K, V, SideInput]#ProcessContext,
        @StateId(CacheKey) cacheState: ValueState[SideInput],
        @TimerId(ExpiryKey) expiryTimer: Timer
    ): Unit = {
      val key = context.element().getKey
      val (values, sideInputs) = context.element().getValue

      cacheSideInputs(sideInputs, cacheState, expiryTimer)
      outputWithSideInput(key, values, context, cacheState)
    }

    @OnTimer(ExpiryKey)
    def onExpiry(@StateId(CacheKey) cacheState: ValueState[SideInput]): Unit = {
      cacheState.clear()
      expiredCounter.inc()
    }

    private def cacheSideInputs(
        sideInputs: Iterable[SideInput],
        cacheState: ValueState[SideInput],
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
        context: SideInputCacheDoFnType[K, V, SideInput]#ProcessContext,
        cacheState: ValueState[SideInput]
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

}
