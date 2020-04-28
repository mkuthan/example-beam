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

  val beginOfWindow = "12:00:00"
  val endOfWindow = "12:10:00"

  val beginOfSecondWindow = "12:10:00"
  val endOfSecondWindow = "12:20:00"

  val ctrZero = (anyScreenId, anyAdCtr.copy(clicks = 0, impressions = 1))
  val ctrOne = (anyScreenId, anyAdCtr.copy(clicks = 1, impressions = 1))
  val ctrUndefined = (anyScreenId, anyAdCtr.copy(clicks = 1, impressions = 0))

  "Impression and then click on-time" should "give ctr 1.0" in runWithContext { sc =>
    val adEvents = testStreamOf[AdEvent]
      .addElementsAtTime("12:01:00", anyImpression)
      .addElementsAtTime("12:02:00", anyClick)
      .advanceWatermarkToInfinity()

    val ctrs = calculateCtrByScreen(sc.testStream(adEvents)).withTimestamp

    ctrs should inOnTimePane(beginOfWindow, endOfWindow) {
      containSingleValue(endOfWindow, ctrOne)
    }
  }

  "Click and then impression on-time" should "give ctr 1.0" in runWithContext { sc =>
    val adEvents = testStreamOf[AdEvent]
      .addElementsAtTime("12:01:00", anyClick)
      .addElementsAtTime("12:02:00", anyImpression)
      .advanceWatermarkToInfinity()

    val ctrs = calculateCtrByScreen(sc.testStream(adEvents)).withTimestamp

    ctrs should inOnTimePane(beginOfWindow, endOfWindow) {
      containSingleValue(endOfWindow, ctrOne)
    }
  }

  "Duplicated impression and click on-time" should "give give ctr 1.0" in runWithContext { sc =>
    val adEvents = testStreamOf[AdEvent]
      .addElementsAtTime("12:01:00", anyImpression, anyImpression)
      .addElementsAtTime("12:02:00", anyClick)
      .advanceWatermarkToInfinity()

    val ctrs = calculateCtrByScreen(sc.testStream(adEvents)).withTimestamp

    ctrs should inOnTimePane(beginOfWindow, endOfWindow) {
      containSingleValue(endOfWindow, ctrOne)
    }
  }

  "Impression and duplicated click on-time" should "give give ctr 1.0" in runWithContext { sc =>
    val adEvents = testStreamOf[AdEvent]
      .addElementsAtTime("12:01:00", anyImpression)
      .addElementsAtTime("12:02:00", anyClick, anyClick)
      .advanceWatermarkToInfinity()

    val ctrs = calculateCtrByScreen(sc.testStream(adEvents)).withTimestamp

    ctrs should inOnTimePane(beginOfWindow, endOfWindow) {
      containSingleValue(endOfWindow, ctrOne)
    }
  }

  "Impression on-time but click out-of-window" should "give ctr 0.0 in impression window and undefined in click window" in runWithContext { sc =>
    val adEvents = testStreamOf[AdEvent]
      .addElementsAtTime("12:01:00", anyImpression)
      .addElementsAtTime("12:11:00", anyClick)
      .advanceWatermarkToInfinity()

    val ctrs = calculateCtrByScreen(sc.testStream(adEvents)).withTimestamp

    ctrs should inOnTimePane(beginOfWindow, endOfWindow) {
      containSingleValue(endOfWindow, ctrZero)
    }

    ctrs should inOnTimePane(beginOfSecondWindow, endOfSecondWindow) {
      containSingleValue(endOfSecondWindow, ctrUndefined)
    }
  }

  "Impression on-time and late click" should "give ctr 0.0" in runWithContext { sc =>
    val adEvents = testStreamOf[AdEvent]
      .addElementsAtTime("12:01:00", anyImpression)
      .advanceWatermarkTo(endOfWindow)
      .addElementsAtTime("12:09:00", anyClick)
      .advanceWatermarkToInfinity()

    val ctrs = calculateCtrByScreen(sc.testStream(adEvents)).withTimestamp

    ctrs should inOnTimePane(beginOfWindow, endOfWindow) {
      containSingleValue(endOfWindow, ctrZero)
    }

    // TODO: missing assertion that late pane is empty
  }

  "Impression on-time and late click but in allowed lateness" should "give ctr 0.0 in on-time pane and 1.0 in on-final pane" in runWithContext { sc =>
    val adEvents = testStreamOf[AdEvent]
      .addElementsAtTime("12:01:00", anyImpression)
      .advanceWatermarkTo(endOfWindow)
      .addElementsAtTime("12:09:00", anyClick)
      .advanceWatermarkToInfinity()

    val allowedLateness = Duration.standardMinutes(2)
    val ctrs = calculateCtrByScreen(sc.testStream(adEvents), allowedLateness = allowedLateness).withTimestamp

    ctrs should inOnTimePane(beginOfWindow, endOfWindow) {
      containSingleValue(endOfWindow, ctrZero)
    }

    // inLatePane or inFinalPane? BTW. inLatePanel has not been implemented in Scio yet
    ctrs should inFinalPane(beginOfWindow, endOfWindow) {
      containSingleValue(endOfWindow, ctrOne)
    }
  }

  "Impression just before click but out-of-window" should "give ctr 0.0 for impression window and undefined for click window" in runWithContext { sc =>
    val adEvents = testStreamOf[AdEvent]
      .addElementsAtTime("12:09:59", anyImpression)
      .addElementsAtTime("12:10:00", anyClick)

      .advanceWatermarkToInfinity()

    val ctrs = calculateCtrByScreen(sc.testStream(adEvents)).withTimestamp

    ctrs should inOnTimePane(beginOfWindow, endOfWindow) {
      containSingleValue(endOfWindow, ctrZero)
    }

    ctrs should inOnTimePane(beginOfSecondWindow, endOfSecondWindow) {
      containSingleValue(endOfSecondWindow, ctrUndefined)
    }
  }

}
