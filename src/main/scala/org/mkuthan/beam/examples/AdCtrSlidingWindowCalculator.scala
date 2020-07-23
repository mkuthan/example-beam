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

import com.spotify.scio.values.SCollection
import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime
import org.apache.beam.sdk.transforms.windowing.AfterWatermark
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration

object AdCtrSlidingWindowCalculator {

  def calculateCtr(
      ctrsByScreen: SCollection[(ScreenId, AdCtr)],
      windowDuration: Duration,
      windowPeriod: Duration,
      allowedLateness: Duration = Duration.ZERO
  ): SCollection[AdCtr] = {
    val windowOptions = WindowOptions(
      allowedLateness = allowedLateness,
      trigger = Repeatedly.forever(
        AfterWatermark
          .pastEndOfWindow()
          .withLateFirings(
            AfterProcessingTime.pastFirstElementInPane()
          )
      ),
      accumulationMode = AccumulationMode.ACCUMULATING_FIRED_PANES
    )

    val ctrs = ctrsByScreen
      .withName(s"Apply sliding window of $windowDuration, period: $windowPeriod and allowed lateness $allowedLateness")
      .withSlidingWindows(windowDuration, windowPeriod, options = windowOptions)
      .withName("Calculate total CTR")
      .sumByKey(AdCtrTotalSemigroup)
      .withName("Discard ScreenId key")
      .values
      .withName(s"Reset sliding window into fixed window of $windowPeriod")
      .withFixedWindows(windowPeriod)

    // ctrs.withPaneInfo.withTimestamp.debug(prefix = "ctr: ")

    ctrs
  }

}
