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
import com.spotify.scio.testing.testStreamOf
import org.mkuthan.beam.TimestampedMatchers

class AdCtrSlidingWindowCalculatorTest extends PipelineSpec with TimestampedMatchers with ModelFixtures {
  import AdCtrSlidingWindowCalculator.calculateCtr
  import org.mkuthan.beam.TestImplicits._

  val beginOfWindow = "12:00:00"
  val endOfWindow = "12:10:00"

  val beginOfSecondWindow = "12:10:00"
  val endOfSecondWindow = "12:20:00"

  val beginOfThirdWindow = "12:20:00"
  val endOfThirdWindow = "12:30:00"

  val beginOfFourthWindow = "12:30:00"
  val endOfFourthWindow = "12:40:00"

  "Running average of ctr 1.0" should "be 1.0, 1.0" in runWithContext { sc =>
    val ctrsByScreen = testStreamOf[(ScreenId, AdCtr)]
      .addElementsAtTime("12:00:01", adCtrOneByScreen)
      .advanceWatermarkToInfinity()

    val ctrs = calculateCtr(sc.testStream(ctrsByScreen))

    ctrs.withTimestamp should inOnTimePane(beginOfWindow, endOfWindow) {
      containSingleValueAtWindowTime(endOfWindow, adCtrOne)
    }

    ctrs.withTimestamp should inOnTimePane(beginOfSecondWindow, endOfSecondWindow) {
      containSingleValueAtWindowTime(endOfSecondWindow, adCtrOne)
    }

    ctrs.withTimestamp should inOnTimePane(beginOfThirdWindow, endOfThirdWindow) { beEmpty }
  }

  "Running average of ctr 1.0 in the first period and 0.0 in the second period" should "be 1.0, 0.5, 0.0" in runWithContext {
    sc =>
      val ctrsByScreen = testStreamOf[(ScreenId, AdCtr)]
        .addElementsAtTime("12:00:01", adCtrOneByScreen)
        .addElementsAtTime("12:10:01", adCtrZeroByScreen)
        .advanceWatermarkToInfinity()

      val ctrs = calculateCtr(sc.testStream(ctrsByScreen))

      ctrs.withTimestamp should inOnTimePane(beginOfWindow, endOfWindow) {
        containSingleValueAtWindowTime(endOfWindow, adCtrOne)
      }

      ctrs.withTimestamp should inOnTimePane(beginOfSecondWindow, endOfSecondWindow) {
        containSingleValueAtWindowTime(endOfSecondWindow, anyAdCtr.copy(clicks = 1, impressions = 2))
      }

      ctrs.withTimestamp should inOnTimePane(beginOfThirdWindow, endOfThirdWindow) {
        containSingleValueAtWindowTime(endOfThirdWindow, adCtrZero)
      }

      ctrs.withTimestamp should inOnTimePane(beginOfFourthWindow, endOfFourthWindow) { beEmpty }
  }
}
