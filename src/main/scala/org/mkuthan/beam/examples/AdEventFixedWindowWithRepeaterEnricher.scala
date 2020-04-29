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
import com.spotify.scio.values.SCollection
import com.spotify.scio.values.SideOutput
import com.spotify.scio.values.WindowOptions
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
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime
import org.apache.beam.sdk.transforms.windowing.AfterWatermark
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration
import org.joda.time.Instant

object AdEventFixedWindowWithRepeaterEnricher {

  val DefaultFixedWindowDuration = Duration.standardMinutes(10)
  val DefaultScreenTtlDuration = Duration.standardMinutes(10)

  def enrichByScreen(
      adEvents: SCollection[AdEvent],
      screens: SCollection[Screen],
      window: Duration = DefaultFixedWindowDuration,
      screenTtl: Duration = DefaultScreenTtlDuration,
      allowedLateness: Duration = Duration.ZERO
  ): (SCollection[(AdEvent, Screen)], SCollection[AdEvent]) = {
    val windowOptions = WindowOptions(
      allowedLateness = allowedLateness,
      trigger = AfterWatermark
        .pastEndOfWindow()
        .withLateFirings(
          AfterProcessingTime.pastFirstElementInPane()
        ),
      accumulationMode = AccumulationMode.ACCUMULATING_FIRED_PANES
    )

    val adEventsByScreenId = adEvents
      .withName("Key AdEvent by ScreenId")
      .keyBy { adEvent => adEvent.screenId }
      .withName(s"Apply fixed window on AdEvent of $window and allowed lateness $allowedLateness")
      .withFixedWindows(duration = window, options = windowOptions)

    val screenByScreenId = screens
      .withName("Key Screen by ScreenId")
      .keyBy { screen => screen.id }
      .applyPerKeyDoFn(new RepeatDoFn(window, screenTtl))
      .withName(s"Apply fixed window on Screen of $window and allowed lateness $allowedLateness")
      .withFixedWindows(duration = window, options = windowOptions)

    val adEventsAndScreen = adEventsByScreenId
      .withName("Join AdEvent with Screen")
      .leftOuterJoin(screenByScreenId)
      .withName("Discard ScreenId join key")
      .values

    val adEventsWithoutScreen = SideOutput[AdEvent]()

    val (adEventsEnriched, sideOutputs) = adEventsAndScreen
      .withSideOutputs(adEventsWithoutScreen)
      .withName("Discard AdEvent without Screen")
      .flatMap {
        case (adEventAndScreen, ctx) =>
          adEventAndScreen match {
            case (adEvent, Some(screen)) => Some((adEvent, screen))
            case (adEvent, None)         => ctx.output(adEventsWithoutScreen, adEvent); None
          }
      }

    (adEventsEnriched, sideOutputs(adEventsWithoutScreen))
  }
}

object RepeatDoFn {
  final val CacheKey = "cache"
  final val LastSeenKey = "lastSeen"
  final val IntervalKey = "interval"
}

class RepeatDoFn[K, V](interval: Duration, ttl: Duration) extends DoFn[KV[K, V], KV[K, V]] {

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
    if (Option(cacheState.read()).isEmpty) {
      timer.set(timestamp.plus(interval))
      receiver.output(element)
    }

    cacheState.write(element)
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
    Option(cacheState.read()).foreach { kv =>
      receiver.output(kv)
      Option(lastSeenState.read()).foreach { lastSeen =>
        if (timestamp.isBefore(lastSeen.plus(ttl))) {
          timer.set(timestamp.plus(interval))
        } else {
          cacheState.clear()
          lastSeenState.clear()
        }
      }
    }
  }
}
