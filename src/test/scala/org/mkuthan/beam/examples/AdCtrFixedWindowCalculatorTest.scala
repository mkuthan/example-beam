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
import org.joda.time.Duration
import org.mkuthan.beam.TimestampedMatchers

class AdCtrFixedWindowCalculatorTest extends PipelineSpec with TimestampedMatchers with ModelFixtures {

  import AdCtrFixedWindowCalculator.calculateCtrByScreen
  import org.mkuthan.beam.TestImplicits._

  val DefaultWindowDuration = Duration.standardMinutes(10)

  val beginOfWindow = "12:00:00"
  val endOfWindow = "12:10:00"

  val beginOfSecondWindow = "12:10:00"
  val endOfSecondWindow = "12:20:00"

  "Impression and then click on-time" should "give ctr 1.0" in runWithContext { sc =>
    val events = testStreamOf[AdEvent]
      .addElementsAtTime("12:00:01", anyImpression)
      .addElementsAtTime("12:00:02", anyClick)
      .advanceWatermarkToInfinity()

    val ctrs = calculateCtrByScreen(sc.testStream(events), DefaultWindowDuration)

    ctrs.withTimestamp should inOnTimePane(beginOfWindow, endOfWindow) {
      containSingleValueAtWindowTime(endOfWindow, adCtrOneByScreen)
    }
  }

  "Click and then impression on-time" should "give ctr 1.0" in runWithContext { sc =>
    val events = testStreamOf[AdEvent]
      .addElementsAtTime("12:00:01", anyClick)
      .addElementsAtTime("12:00:02", anyImpression)
      .advanceWatermarkToInfinity()

    val ctrs = calculateCtrByScreen(sc.testStream(events), DefaultWindowDuration)

    ctrs.withTimestamp should inOnTimePane(beginOfWindow, endOfWindow) {
      containSingleValueAtWindowTime(endOfWindow, adCtrOneByScreen)
    }
  }

  "Duplicated impression and click on-time" should "give give ctr 1.0" in runWithContext { sc =>
    val events = testStreamOf[AdEvent]
      .addElementsAtTime("12:00:01", anyImpression, anyImpression)
      .addElementsAtTime("12:00:02", anyClick)
      .advanceWatermarkToInfinity()

    val ctrs = calculateCtrByScreen(sc.testStream(events), DefaultWindowDuration)

    ctrs.withTimestamp should inOnTimePane(beginOfWindow, endOfWindow) {
      containSingleValueAtWindowTime(endOfWindow, adCtrOneByScreen)
    }
  }

  "Impression and duplicated click on-time" should "give give ctr 1.0" in runWithContext { sc =>
    val events = testStreamOf[AdEvent]
      .addElementsAtTime("12:00:01", anyImpression)
      .addElementsAtTime("12:00:02", anyClick, anyClick)
      .advanceWatermarkToInfinity()

    val ctrs = calculateCtrByScreen(sc.testStream(events), DefaultWindowDuration)

    ctrs.withTimestamp should inOnTimePane(beginOfWindow, endOfWindow) {
      containSingleValueAtWindowTime(endOfWindow, adCtrOneByScreen)
    }
  }

  "Impression on-time and click out-of-window" should "give ctr 0.0 in impression window and undefined in click window" in runWithContext {
    sc =>
      val events = testStreamOf[AdEvent]
        .addElementsAtTime("12:00:01", anyImpression)
        .addElementsAtTime("12:10:01", anyClick)
        .advanceWatermarkToInfinity()

      val ctrs = calculateCtrByScreen(sc.testStream(events), DefaultWindowDuration)

      ctrs.withTimestamp should inOnTimePane(beginOfWindow, endOfWindow) {
        containSingleValueAtWindowTime(endOfWindow, adCtrZeroByScreen)
      }

      ctrs.withTimestamp should inOnTimePane(beginOfSecondWindow, endOfSecondWindow) {
        containSingleValueAtWindowTime(endOfSecondWindow, adCtrUndefinedByScreen)
      }
  }

  "Impression on-time and late click" should "give ctr 0.0 in on-time pane and late pane is empty" in runWithContext {
    sc =>
      val events = testStreamOf[AdEvent]
        .addElementsAtTime("12:00:01", anyImpression)
        .advanceWatermarkTo(endOfWindow) // ensure that the next click will be considered late
        .addElementsAtTime("12:09:59", anyClick)
        .advanceWatermarkToInfinity()

      val ctrs = calculateCtrByScreen(sc.testStream(events), DefaultWindowDuration)

      ctrs.withTimestamp should inOnTimePane(beginOfWindow, endOfWindow) {
        containSingleValueAtWindowTime(endOfWindow, adCtrZeroByScreen)
      }

      ctrs.withTimestamp should inLatePane(beginOfWindow, endOfWindow) { beEmpty }
  }

  "Impression on-time and late click but in allowed lateness" should "give ctr 0.0 in on-time pane and 1.0 in late pane" in runWithContext {
    sc =>
      val events = testStreamOf[AdEvent]
        .addElementsAtTime("12:00:01", anyImpression)
        .advanceWatermarkTo(endOfWindow) // ensure that the next click will be considered late
        .addElementsAtTime("12:09:59", anyClick)
        .advanceWatermarkToInfinity()

      val allowedLateness = Duration.standardMinutes(1)
      val ctrs = calculateCtrByScreen(sc.testStream(events), DefaultWindowDuration, allowedLateness)

      ctrs.withTimestamp should inOnTimePane(beginOfWindow, endOfWindow) {
        containSingleValueAtWindowTime(endOfWindow, adCtrZeroByScreen)
      }

      ctrs.withTimestamp should inLatePane(beginOfWindow, endOfWindow) {
        containSingleValueAtWindowTime(endOfWindow, adCtrOneByScreen)
      }
  }

  "Late impression and click but in allowed lateness" should "give ctr 1.0 in late pane and on-time pane is empty" in runWithContext {
    sc =>
      val events = testStreamOf[AdEvent]
        .advanceWatermarkTo(endOfWindow) // ensure that the next events will be considered late
        .addElementsAtTime("12:09:59", anyClick, anyImpression)
        .advanceWatermarkToInfinity()

      val allowedLateness = Duration.standardMinutes(1)
      val ctrs = calculateCtrByScreen(sc.testStream(events), DefaultWindowDuration, allowedLateness)

      ctrs.withTimestamp should inOnTimePane(beginOfWindow, endOfWindow) {
        beEmpty
      }

      ctrs.withTimestamp should inLatePane(beginOfWindow, endOfWindow) {
        containSingleValueAtWindowTime(endOfWindow, adCtrOneByScreen)
      }
  }

  "Impression just before click but out-of-window" should "give ctr 0.0 for impression window and undefined for click window" in runWithContext {
    sc =>
      val events = testStreamOf[AdEvent]
        .addElementsAtTime("12:09:59", anyImpression)
        .addElementsAtTime("12:10:00", anyClick)
        .advanceWatermarkToInfinity()

      val ctrs = calculateCtrByScreen(sc.testStream(events), DefaultWindowDuration)

      ctrs.withTimestamp should inOnTimePane(beginOfWindow, endOfWindow) {
        containSingleValueAtWindowTime(endOfWindow, adCtrZeroByScreen)
      }

      ctrs.withTimestamp should inOnTimePane(beginOfSecondWindow, endOfSecondWindow) {
        containSingleValueAtWindowTime(endOfSecondWindow, adCtrUndefinedByScreen)
      }
  }

}
