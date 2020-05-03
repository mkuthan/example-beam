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

class ScreenGlobalWindowWithSideInputEnricherTest extends PipelineSpec with TimestampedMatchers with ModelFixtures {

  import ScreenGlobalWindowWithSideInputEnricher.enrichByPublication
  import org.mkuthan.beam.TestImplicits._

  "Screen" should "be enriched by publication" in runWithContext { sc =>
    val screens = testStreamOf[Screen]
      .advanceWatermarkTo("13:00:00") // ensure that publication has been seen
      .addElementsAtTime("13:00:00", anyScreen)
      .advanceWatermarkToInfinity()

    val publications = testStreamOf[Publication]
      .addElementsAtTime("12:00:00", anyPublication)
      .advanceWatermarkToInfinity()

    val (enriched, dlq) = enrichByPublication(sc.testStream(screens), sc.testStream(publications))

    enriched.withTimestamp should containSingleValueAtTime("13:00:00", (anyScreen, anyPublication))
    dlq should beEmpty
  }

  // TODO: why late screen is enriched by publication?
  "Late screen" should "not be enriched by publication but it was, WTF???" in runWithContext { sc =>
    val screens = testStreamOf[Screen]
      .addElementsAtTime("11:59:59", anyScreen)
      .advanceWatermarkToInfinity()

    val publications = testStreamOf[Publication]
      .advanceWatermarkTo("12:00:00") // ensure that screen has been seen
      .addElementsAtTime("12:00:00", anyPublication)
      .advanceWatermarkToInfinity()

    val (enriched, dlq) = enrichByPublication(sc.testStream(screens), sc.testStream(publications))

    enriched.withTimestamp should containSingleValueAtTime("11:59:59", (anyScreen, anyPublication))
    dlq.withTimestamp should beEmpty
  }

  // TODO: why the second publication is not observed in SideInput when the screen is processed?
  ignore should "be enriched by latest ordered publication" in runWithContext { sc =>
    val screens = testStreamOf[Screen]
      .advanceWatermarkTo("13:00:00") // ensure that publication has been seen
      .addElementsAtTime("13:00:00", anyScreen)
      .advanceWatermarkToInfinity()

    val firstPublication = anyPublication.copy(version = "first")
    val secondPublication = anyPublication.copy(version = "second")

    val publications = testStreamOf[Publication]
      .addElementsAtTime("12:00:00", firstPublication)
      .addElementsAtTime("12:00:01", secondPublication)
      .advanceWatermarkToInfinity()

    val (enriched, dlq) = enrichByPublication(sc.testStream(screens), sc.testStream(publications))

    enriched.withTimestamp should containSingleValueAtTime("13:00:00", (anyScreen, secondPublication))
    dlq should beEmpty
  }

  // TODO: why the second publication is not observed in SideInput when the screen is processed?
  ignore should "be enriched by latest unordered publication" in runWithContext { sc =>
    val screens = testStreamOf[Screen]
      .advanceWatermarkTo("13:00:00") // ensure that publication has been seen
      .addElementsAtTime("13:00:00", anyScreen)
      .advanceWatermarkToInfinity()

    val firstPublication = anyPublication.copy(version = "first")
    val secondPublication = anyPublication.copy(version = "second")

    val publications = testStreamOf[Publication]
      .addElementsAtTime("12:00:01", secondPublication)
      .addElementsAtTime("12:00:00", firstPublication)
      .advanceWatermarkToInfinity()

    val (enriched, dlq) = enrichByPublication(sc.testStream(screens), sc.testStream(publications))

    enriched.withTimestamp should containSingleValueAtTime("13:00:00", (anyScreen, secondPublication))
    dlq should beEmpty
  }

}
