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
import org.apache.beam.sdk.transforms.windowing._
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration

object AdCtrFixedWindowCalculator {

  def calculateCtrByScreen(
      events: SCollection[AdEvent],
      windowDuration: Duration,
      allowedLateness: Duration = Duration.ZERO
  ): SCollection[(ScreenId, AdCtr)] = {
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

    val ctrsByScreen = events
      .withName("Key AdEvent by AdId/ScreenId")
      .keyBy { adEvent => (adEvent.id, adEvent.screenId) }
      .withName("Prepare initial AdCtr from AdEvent")
      .mapValues { adEvent => AdCtr.fromAdEvent(adEvent) }
      .withName(s"Apply fixed window of $windowDuration and allowed lateness $allowedLateness")
      .withFixedWindows(duration = windowDuration, options = windowOptions)
      .withName("Calculate capped CTR per ScreenId")
      .sumByKey(AdCtrCappedSemigroup)
      .withName("Discard AdId/ScreenId key")
      .mapKeys { case (_, screenId) => screenId }

    // ctrsByScreen.withPaneInfo.withTimestamp.debug(prefix = "ctr by screen: ")

    ctrsByScreen
  }
}
