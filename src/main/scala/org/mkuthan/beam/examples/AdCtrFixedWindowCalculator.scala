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

  val DefaultFixedWindowDuration = Duration.standardMinutes(10)

  def calculateCtr(
      adEvents: SCollection[AdEvent],
      window: Duration = DefaultFixedWindowDuration,
      allowedLateness: Duration = Duration.ZERO
  ): SCollection[AdCtr] = {
    implicit val adCtrSemigroup = new AdCtrSemigroup

    val windowOptions = WindowOptions(
      allowedLateness = allowedLateness,
      trigger = AfterWatermark
        .pastEndOfWindow()
        .withLateFirings(
          AfterProcessingTime.pastFirstElementInPane()
        ),
      accumulationMode = AccumulationMode.ACCUMULATING_FIRED_PANES
    )

    adEvents
      .withName("Prepare initial AdCtr from AdEvent")
      .map { adEvent =>
        adEvent.action match {
          case AdAction.Click => AdCtr.click(adEvent.id)
          case AdAction.Impression => AdCtr.impression(adEvent.id)
          case _ => AdCtr.unknown(adEvent.id)
        }
      }
      .withName("Key AdCtr by AdId")
      .keyBy { adCtr => adCtr.id }
      .withName(s"Apply fixed window of $window and allowed lateness $allowedLateness")
      .withFixedWindows(duration = window, options = windowOptions)
      .withName("Calculate CTR")
      .sumByKey
      .withName("Discard AdId key")
      .values
  }
}
