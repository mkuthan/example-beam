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

// TODO: Add inOnTimePane asserts, they do not work for custom window and I've not figured out why, yet.
class AdCtrCustomWindowCalculatorTest extends PipelineSpec with TimestampedMatchers with ModelFixtures {

  import AdCtrCustomWindowCalculator.calculateCtrByScreen
  import org.mkuthan.beam.TestImplicits._

  "Impression and then click on-time" should "give ctr 1.0" in runWithContext { sc =>
    val impressionTime = "12:00:00"
    val clickTime = "12:00:01"

    val events = testStreamOf[AdEvent]
      .addElementsAtTime(impressionTime, anyImpression)
      .addElementsAtTime(clickTime, anyClick)
      .advanceWatermarkToInfinity()

    val ctrs = calculateCtrByScreen(sc.testStream(events))

    ctrs.withTimestamp should containSingleValueAtTime(clickTime, adCtrOneByScreen)
  }

  "Impression and then click out-of-window" should "give ctr 0.0 for impression window and undefined for click window" in runWithContext {
    sc =>
      val impressionTime = "12:00:00"
      val clickTime = "12:11:00"

      val events = testStreamOf[AdEvent]
        .addElementsAtTime(impressionTime, anyImpression)
        .advanceWatermarkTo("12:10:01") // ensure that impression has been expired
        .addElementsAtTime(clickTime, anyClick)
        .advanceWatermarkToInfinity()

      val ctrs = calculateCtrByScreen(sc.testStream(events))

      ctrs.withTimestamp should containInAnyOrderAtTime(
        Seq(
          ("12:10:00", adCtrZeroByScreen), // ten minutes impression to click window
          ("12:12:00", adCtrUndefinedByScreen) // one minute click to impression window
        )
      )
  }

  "Click and then impression on-time" should "give ctr 1.0" in runWithContext { sc =>
    val clickTime = "12:00:00"
    val impressionTime = "12:00:01"

    val events = testStreamOf[AdEvent]
      .addElementsAtTime(clickTime, anyClick)
      .addElementsAtTime(impressionTime, anyImpression)
      .advanceWatermarkToInfinity()

    val ctrs = calculateCtrByScreen(sc.testStream(events))

    ctrs.withTimestamp should containSingleValueAtTime(impressionTime, adCtrOneByScreen)
  }

  "Click and then impression out-of-window" should "give ctr undefined for click window and 0.0 for impression window" in runWithContext {
    sc =>
      val clickTime = "12:00:00"
      val impressionTime = "12:02:00"

      val events = testStreamOf[AdEvent]
        .addElementsAtTime(clickTime, anyClick)
        .advanceWatermarkTo("12:01:01") // ensure that click has been expired
        .addElementsAtTime(impressionTime, anyImpression)
        .advanceWatermarkToInfinity()

      val ctrs = calculateCtrByScreen(sc.testStream(events))

      ctrs.withTimestamp should containInAnyOrderAtTime(
        Seq(
          ("12:01:00", adCtrUndefinedByScreen), // one minute click to impression window
          ("12:12:00", adCtrZeroByScreen) // ten minutes impression to click window
        )
      )
  }
}
