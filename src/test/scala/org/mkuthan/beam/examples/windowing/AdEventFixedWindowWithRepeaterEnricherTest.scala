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

package org.mkuthan.beam.examples.windowing

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing._
import org.mkuthan.beam.examples.TimestampedMatchers

class AdEventFixedWindowWithRepeaterEnricherTest extends PipelineSpec with TimestampedMatchers with ModelFixtures {

  import org.mkuthan.beam.examples.TestImplicits._
  import org.mkuthan.beam.examples.windowing.AdEventFixedWindowWithRepeaterEnricher.enrichByScreen

  val beginOfWindow = "12:00:00"
  val endOfWindow = "12:10:00"

  val beginOfSecondWindow = "12:10:00"
  val endOfSecondWindow = "12:20:00"

  val beginOfThirdWindow = "12:20:00"
  val endOfThirdWindow = "12:30:00"

  "AdEvent" should "be enriched by screen" in runWithContext { sc =>
    val events = testStreamOf[AdEvent]
      .advanceWatermarkTo("12:00:00") // ensure that screen has been seen
      .addElementsAtTime("12:00:01", anyAdEvent)
      .advanceWatermarkToInfinity()

    val screens = testStreamOf[Screen]
      .addElementsAtTime("12:00:00", anyScreen)
      .advanceWatermarkToInfinity()

    val (enriched, dlq) = enrichByScreen(sc.testStream(events), sc.testStream(screens))

    enriched.withTimestamp should inOnTimePane(beginOfWindow, endOfWindow) {
      containSingleValueAtWindowTime(endOfWindow, (anyAdEvent, anyScreen))
    }

    dlq should beEmpty
  }

  "AdEvent" should "be enriched by screen after event" in runWithContext { sc =>
    val events = testStreamOf[AdEvent]
      .addElementsAtTime("12:00:00", anyAdEvent)
      .advanceWatermarkToInfinity()

    val screens = testStreamOf[Screen]
      .advanceWatermarkTo("12:00:01") // ensure that Ad event has been seen
      .addElementsAtTime("12:00:01", anyScreen)
      .advanceWatermarkToInfinity()

    val (enriched, dlq) = enrichByScreen(sc.testStream(events), sc.testStream(screens))

    enriched.withTimestamp should inOnTimePane(beginOfWindow, endOfWindow) {
      containSingleValueAtWindowTime(endOfWindow, (anyAdEvent, anyScreen))
    }

    dlq should beEmpty
  }

  "AdEvent" should "be enriched by repeated screen" in runWithContext { sc =>
    val events = testStreamOf[AdEvent]
      .advanceWatermarkTo("12:10:00") // ensure that screen has been seen
      .addElementsAtTime("12:10:00", anyAdEvent)
      .advanceWatermarkToInfinity()

    val screens = testStreamOf[Screen]
      .addElementsAtTime("12:09:59", anyScreen)
      .advanceWatermarkToInfinity()

    val (enriched, dlq) = enrichByScreen(sc.testStream(events), sc.testStream(screens))

    enriched.withTimestamp should inOnTimePane(beginOfSecondWindow, endOfSecondWindow) {
      containSingleValueAtWindowTime(endOfSecondWindow, (anyAdEvent, anyScreen))
    }

    dlq should beEmpty
  }

  "AdEvent" should "not be enriched by expired screen" in runWithContext { sc =>
    val events = testStreamOf[AdEvent]
      .advanceWatermarkTo("12:20:00") // ensure that screen has been expired
      .addElementsAtTime("12:20:01", anyAdEvent)
      .advanceWatermarkToInfinity()

    val screens = testStreamOf[Screen]
      .addElementsAtTime("12:00:01", anyScreen)
      .advanceWatermarkToInfinity()

    val (enriched, dlq) = enrichByScreen(sc.testStream(events), sc.testStream(screens))

    enriched should beEmpty

    dlq.withTimestamp should inOnTimePane(beginOfThirdWindow, endOfThirdWindow) {
      containSingleValueAtWindowTime(endOfThirdWindow, anyAdEvent)
    }
  }
}
