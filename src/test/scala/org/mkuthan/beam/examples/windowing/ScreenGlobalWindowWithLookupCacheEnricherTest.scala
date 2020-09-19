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

package org.mkuthan.beam.examples.windowing

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing._
import org.mkuthan.beam.examples.TimestampedMatchers

class ScreenGlobalWindowWithLookupCacheEnricherTest extends PipelineSpec with TimestampedMatchers with ModelFixtures {

  import org.mkuthan.beam.examples.TestImplicits._
  import org.mkuthan.beam.examples.windowing.ScreenGlobalWindowWithLookupCacheEnricher.enrichByPublication

  "Screen" should "be enriched by cached publication" in runWithContext { sc =>
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

  "Late screen" should "be enriched by cached publication" in runWithContext { sc =>
    val screens = testStreamOf[Screen]
      .addElementsAtTime("11:59:59", anyScreen)
      .advanceWatermarkToInfinity()

    val publications = testStreamOf[Publication]
      .advanceWatermarkTo("12:00:00") // ensure that screen has been seen
      .addElementsAtTime("12:00:00", anyPublication)
      .advanceWatermarkToInfinity()

    val (enriched, dlq) = enrichByPublication(sc.testStream(screens), sc.testStream(publications))

    // TODO: maybe it should be emitted with original screen timestamp instaed of latet seen timestamp?
    enriched.withTimestamp should containSingleValueAtTime("12:00:00", (anyScreen, anyPublication))
    dlq should beEmpty
  }

  "Too late screen" should "not be enriched by cached publication" in runWithContext { sc =>
    val screens = testStreamOf[Screen]
      .addElementsAtTime("10:59:59", anyScreen)
      .advanceWatermarkToInfinity()

    val publications = testStreamOf[Publication]
      .advanceWatermarkTo("12:00:00") // ensure that screen has been seen
      .addElementsAtTime("12:00:00", anyPublication)
      .advanceWatermarkToInfinity()

    val (enriched, dlq) = enrichByPublication(sc.testStream(screens), sc.testStream(publications))

    enriched should beEmpty
    // TODO: maybe it should be emitted with latest seen timestamp instead of screen original timestamp?
    dlq.withTimestamp should containSingleValueAtTime("10:59:59", anyScreen)
  }

  "Publication has expired, so screen" should "not be enriched" in runWithContext { sc =>
    val screens = testStreamOf[Screen]
      .advanceWatermarkTo("13:00:01") // ensure that publication has been expired
      .addElementsAtTime("13:00:01", anyScreen)
      .advanceWatermarkToInfinity()

    val publications = testStreamOf[Publication]
      .addElementsAtTime("12:00:00", anyPublication)
      .advanceWatermarkToInfinity()

    val (enriched, dlq) = enrichByPublication(sc.testStream(screens), sc.testStream(publications))

    enriched should beEmpty
    dlq.withTimestamp should containSingleValueAtTime("13:00:01", anyScreen)
  }

  // publications with the same timestamp are unordered (and modeled as Iterable passed to the ParDo)
  ignore should "be enriched by last publication" in runWithContext { sc =>
    val screens = testStreamOf[Screen]
      .advanceWatermarkTo("13:00:00") // ensure that publication has been seen
      .addElementsAtTime("13:00:00", anyScreen)
      .advanceWatermarkToInfinity()

    val firstPublication = anyPublication.copy(version = "first")
    val secondPublication = anyPublication.copy(version = "second")

    val publications = testStreamOf[Publication]
      .addElementsAtTime("12:00:00", firstPublication, secondPublication)
      .advanceWatermarkToInfinity()

    val (enriched, dlq) = enrichByPublication(sc.testStream(screens), sc.testStream(publications))

    enriched.withTimestamp should containSingleValueAtTime("13:00:00", (anyScreen, secondPublication))
    dlq should beEmpty
  }

  "Screen" should "be enriched by latest ordered publication" in runWithContext { sc =>
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

  "Screen" should "be enriched by latest unordered publication" in runWithContext { sc =>
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
