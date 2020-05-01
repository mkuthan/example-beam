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

package org.mkuthan.beam.examples

import com.spotify.scio.coders.Coder
import com.spotify.scio.coders.CoderMaterializer
import com.typesafe.scalalogging.LazyLogging
import org.apache.beam.sdk.state.StateSpecs
import org.apache.beam.sdk.state.TimeDomain
import org.apache.beam.sdk.state.Timer
import org.apache.beam.sdk.state.TimerSpecs
import org.apache.beam.sdk.state.ValueState
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.AlwaysFetched
import org.apache.beam.sdk.transforms.DoFn.Element
import org.apache.beam.sdk.transforms.DoFn.OnTimer
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.DoFn.StateId
import org.apache.beam.sdk.transforms.DoFn.TimerId
import org.apache.beam.sdk.transforms.DoFn.Timestamp
import org.apache.beam.sdk.values.KV
import org.joda.time.Duration
import org.joda.time.Instant

object RepeatDoFn {
  final val CacheKey = "Cache"
  final val LastSeenKey = "LastSeen"
  final val IntervalKey = "Interval"
}

class RepeatDoFn[K, V](interval: Duration, ttl: Duration) extends DoFn[KV[K, V], KV[K, V]] with LazyLogging {

  import RepeatDoFn._

  // noinspection ScalaUnusedSymbol
  @StateId(CacheKey) private val cacheSpec =
    StateSpecs.value[KV[K, V]](CoderMaterializer.beamWithDefault(Coder[KV[K, V]]))

  // noinspection ScalaUnusedSymbol
  @StateId(LastSeenKey) private val lastSeenSpec =
    StateSpecs.value[Instant](CoderMaterializer.beamWithDefault(Coder[Instant]))

  // noinspection ScalaUnusedSymbol
  @TimerId(IntervalKey) private val triggerIntervalSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME)

  @ProcessElement
  def processElement(
      @Timestamp timestamp: Instant,
      @Element element: KV[K, V],
      @AlwaysFetched @StateId(CacheKey) cacheState: ValueState[KV[K, V]],
      @AlwaysFetched @StateId(LastSeenKey) lastSeenState: ValueState[Instant],
      @TimerId(IntervalKey) timer: Timer,
      receiver: OutputReceiver[KV[K, V]]
  ): Unit = {
    logger.debug("Process: {}, {}", timestamp, element)

    // if elements hasn't been seen before
    if (Option(cacheState.read()).isEmpty) {
      // initial emission
      outputValue(timestamp, element, receiver)
      // schedule repeated emission
      timer.set(timestamp.plus(interval))
    }

    // save element for repeated emission
    cacheState.write(element)

    // save when element has been seen
    lastSeenState.write(timestamp)
  }

  @OnTimer(IntervalKey)
  def onTriggerInterval(
      @Timestamp timestamp: Instant,
      @AlwaysFetched @StateId(CacheKey) cacheState: ValueState[KV[K, V]],
      @AlwaysFetched @StateId(LastSeenKey) lastSeenState: ValueState[Instant],
      @TimerId(IntervalKey) timer: Timer,
      receiver: OutputReceiver[KV[K, V]]
  ): Unit = {
    logger.debug("TriggerTimer: {}", timestamp)

    Option(cacheState.read()).foreach { element =>
      // repeated emission
      outputValue(timestamp, element, receiver)

      Option(lastSeenState.read()).foreach { lastSeen =>
        if (timestamp.isBefore(lastSeen.plus(ttl))) {
          // before TTL, schedule next repeated emission
          timer.set(timestamp.plus(interval))
        } else {
          // after TTL, clear the state
          cacheState.clear()
          lastSeenState.clear()
        }
      }
    }
  }

  private def outputValue(timestamp: Instant, element: KV[K, V], receiver: OutputReceiver[KV[K, V]]): Unit = {
    logger.debug("Output: {}, {}", timestamp, element)
    receiver.outputWithTimestamp(element, timestamp)
  }
}
