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

  import AdCtrFixedWindowCalculator.calculateCtr
  import org.mkuthan.beam.TestImplicits._

  behavior of "Ctr in fixed window"

  val beginOfWindow = "12:00:00"
  val endOfWindow = "12:10:00"

  it should "be calculated for impression and click on-time" in runWithContext { sc =>
    val adEvents = testStreamOf[AdEvent]
      .addElementsAtTime("12:01:00", anyImpression)
      .addElementsAtTime("12:02:00", anyClick)
      .advanceWatermarkToInfinity()

    val adCtrs = calculateCtr(sc.testStream(adEvents)).withTimestamp

    adCtrs should inOnTimePane(beginOfWindow, endOfWindow) {
      containSingleValue("12:10:00", anyAdCtr.copy(clicks = 1, impressions = 1))
    }
  }

  it should "not be calculated for impression and click out of window" in runWithContext { sc =>
    val adEvents = testStreamOf[AdEvent]
      .addElementsAtTime("12:01:00", anyImpression)
      .addElementsAtTime("12:11:00", anyClick)
      .advanceWatermarkToInfinity()

    val adCtrs = calculateCtr(sc.testStream(adEvents)).withTimestamp

    adCtrs should inOnTimePane(beginOfWindow, endOfWindow) {
      containSingleValue(endOfWindow, anyAdCtr.copy(clicks = 0, impressions = 1))
    }
  }

  it should "be calculated for impression after click on-time" in runWithContext { sc =>
    val adEvents = testStreamOf[AdEvent]
      .addElementsAtTime("12:01:00", anyClick)
      .addElementsAtTime("12:02:00", anyImpression)
      .advanceWatermarkToInfinity()

    val adCtrs = calculateCtr(sc.testStream(adEvents)).withTimestamp

    adCtrs should inOnTimePane(beginOfWindow, endOfWindow) {
      containSingleValue(endOfWindow, anyAdCtr.copy(clicks = 1, impressions = 1))
    }
  }

  it should "be calculated for impression and late click but in allowed lateness" in runWithContext { sc =>
    val adEvents = testStreamOf[AdEvent]
      .addElementsAtTime("12:01:00", anyImpression)
      .advanceWatermarkTo("12:10:00")
      .addElementsAtTime("12:09:00", anyClick)
      .advanceWatermarkToInfinity()

    val allowedLateness = Duration.standardMinutes(2)
    val adCtrs = calculateCtr(sc.testStream(adEvents), allowedLateness = allowedLateness).withTimestamp

    adCtrs should inOnTimePane(beginOfWindow, endOfWindow) {
      containSingleValue(endOfWindow, anyAdCtr.copy(clicks = 0, impressions = 1))
    }

    // TODO: inLatePane, inFinalPane or both?
    adCtrs should inFinalPane(beginOfWindow, endOfWindow) {
      containSingleValue("12:10:00", anyAdCtr.copy(clicks = 1, impressions = 1))
    }
  }

  it should "not be calculated for impression just before click but out of window" in runWithContext { sc =>
    val adEvents = testStreamOf[AdEvent]
      .addElementsAtTime("12:09:59", anyImpression)
      .addElementsAtTime("12:10:00", anyClick)

      .advanceWatermarkToInfinity()

    val adCtrs = calculateCtr(sc.testStream(adEvents)).withTimestamp

    adCtrs should inOnTimePane(beginOfWindow, endOfWindow) {
      containSingleValue(endOfWindow, anyAdCtr.copy(clicks = 0, impressions = 1))
    }

    adCtrs should inOnTimePane("12:10:00", "12:20:00") {
      containSingleValue("12:20:00", anyAdCtr.copy(clicks = 1, impressions = 0))
    }
  }

}
