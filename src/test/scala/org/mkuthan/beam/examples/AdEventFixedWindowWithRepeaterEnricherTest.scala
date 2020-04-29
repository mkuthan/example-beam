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

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing._
import org.mkuthan.beam.TimestampedMatchers

class AdEventFixedWindowWithRepeaterEnricherTest extends PipelineSpec with TimestampedMatchers with ModelFixtures {

  import AdEventFixedWindowWithRepeaterEnricher.enrichByScreen
  import org.mkuthan.beam.TestImplicits._

  val beginOfWindow = "12:00:00"
  val endOfWindow = "12:10:00"

  val beginOfSecondWindow = "12:10:00"
  val endOfSecondWindow = "12:20:00"

  val beginOfThirdWindow = "12:20:00"
  val endOfThirdWindow = "12:30:00"

  "Screen and then AdEvent in the same window" should "be enriched" in runWithContext { sc =>
    val adEvents = testStreamOf[AdEvent]
      .addElementsAtTime("12:02:00", anyAdEvent)
      .advanceWatermarkToInfinity()

    val adScreens = testStreamOf[Screen]
      .addElementsAtTime("12:01:00", anyScreen)
      .advanceWatermarkToInfinity()

    val (enriched, dlq) = enrichByScreen(sc.testStream(adEvents), sc.testStream(adScreens))

    enriched.withTimestamp should inOnTimePane(beginOfWindow, endOfWindow) {
      containSingleValue(endOfWindow, (anyAdEvent, anyScreen))
    }

    dlq should beEmpty
  }

  "Screen and then AdEvent in the next window" should "be enriched by repeated screen" in runWithContext { sc =>
    val adEvents = testStreamOf[AdEvent]
      .addElementsAtTime("12:12:00", anyAdEvent)
      .advanceWatermarkToInfinity()

    val adScreens = testStreamOf[Screen]
      .addElementsAtTime("12:01:00", anyScreen)
      .advanceWatermarkToInfinity()

    val (enriched, dlq) = enrichByScreen(sc.testStream(adEvents), sc.testStream(adScreens))

    enriched.withTimestamp should inOnTimePane(beginOfSecondWindow, endOfSecondWindow) {
      containSingleValue(endOfSecondWindow, (anyAdEvent, anyScreen))
    }

    dlq should beEmpty
  }

  "Screen and then very close AdEvent but in the next window" should "be enriched by repeated screen" in runWithContext {
    sc =>
      val adEvents = testStreamOf[AdEvent]
        .addElementsAtTime("12:10:00", anyAdEvent)
        .advanceWatermarkToInfinity()

      val adScreens = testStreamOf[Screen]
        .addElementsAtTime("12:09:59", anyScreen)
        .advanceWatermarkToInfinity()

      val (enriched, dlq) = enrichByScreen(sc.testStream(adEvents), sc.testStream(adScreens))

      enriched.withTimestamp should inOnTimePane(beginOfSecondWindow, endOfSecondWindow) {
        containSingleValue(endOfSecondWindow, (anyAdEvent, anyScreen))
      }

      dlq should beEmpty
  }

  "Screen and then AdEvent in the far future window" should "not be enriched" in runWithContext { sc =>
    val adEvents = testStreamOf[AdEvent]
      .advanceWatermarkTo("12:20:00") // expire repeater cache at ttl
      .addElementsAtTime("12:22:00", anyAdEvent)
      .advanceWatermarkToInfinity()

    val adScreens = testStreamOf[Screen]
      .addElementsAtTime("12:01:00", anyScreen)
      .advanceWatermarkToInfinity()

    val (enriched, dlq) = enrichByScreen(sc.testStream(adEvents), sc.testStream(adScreens))

    enriched should beEmpty

    dlq.withTimestamp should inOnTimePane(beginOfThirdWindow, endOfThirdWindow) {
      containSingleValue(endOfThirdWindow, anyAdEvent)
    }
  }

}
